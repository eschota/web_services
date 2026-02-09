/**
 * QwertyStock VPN - Background Service Worker
 */

importScripts('../utils/api.js');

const api = new GatewayAPI();
let currentProxy = null;
let isEnabled = false;
let manualVpsId = null;
let isInitialized = false;
let gatewayFailCount = 0;
const MAX_GATEWAY_FAILS = 3;

async function ensureInitialized() {
  if (isInitialized) return;
  
  const data = await chrome.storage.local.get(['isEnabled', 'currentProxy', 'apiKey', 'clientId', 'manualVpsId']);
  isEnabled = data.isEnabled || false;
  currentProxy = data.currentProxy || null;
  manualVpsId = data.manualVpsId || null;
  
  if (data.apiKey) api.apiKey = data.apiKey;
  if (data.clientId) api.clientId = data.clientId;
  
  console.log('SW state restored, isEnabled:', isEnabled, 'proxy:', currentProxy ? 'yes' : 'no', 'manualVpsId:', manualVpsId);
  isInitialized = true;
}

// Initialize on install
chrome.runtime.onInstalled.addListener(async () => {
  await chrome.storage.local.set({
    isEnabled: false,
    currentProxy: null,
    gatewayStatus: null,
    vpsStats: [],
    domainRules: { domains: [], mode: 'whitelist' },
    apiKey: 'qvpn_master_key_2026',
    clientId: 'test_client',
    manualVpsId: null
  });
  await api.init();
  console.log('QwertyStock VPN extension installed');
});

// Startup
chrome.runtime.onStartup?.addListener(async () => {
  await ensureInitialized();
  if (isEnabled && currentProxy) {
    await applyProxy();
  }
});

// Alarm for periodic refresh
chrome.alarms.create('refreshStatus', { periodInMinutes: 1 });
chrome.alarms.create('refreshStats', { periodInMinutes: 2 });

chrome.alarms.onAlarm.addListener(async (alarm) => {
  await ensureInitialized();
  await api.init();
  if (alarm.name === 'refreshStatus' || alarm.name === 'reconnectRetry') {
    await refreshGatewayStatus();
    
    // If we were trying to reconnect and it succeeded, re-enable proxy
    if (alarm.name === 'reconnectRetry' && gatewayFailCount === 0) {
      const data = await chrome.storage.local.get(['currentProxy']);
      if (data.currentProxy) {
        isEnabled = true;
        await chrome.storage.local.set({ isEnabled: true });
        await applyProxy();
      }
    }
  } else if (alarm.name === 'refreshStats') {
    await refreshVpsStats();
  }
});

async function refreshGatewayStatus() {
  try {
    const status = await api.getStatus();
    await chrome.storage.local.set({ gatewayStatus: status });
    gatewayFailCount = 0; // Reset on success

    if (status.gateway_status !== 'up' && isEnabled) {
      gatewayFailCount = MAX_GATEWAY_FAILS; // Force disable logic
      await handleGatewayFailure('Gateway reported down');
    }
  } catch (error) {
    console.warn('Failed to fetch gateway status:', error.message);
    gatewayFailCount++;
    
    if (gatewayFailCount >= MAX_GATEWAY_FAILS && isEnabled) {
      await handleGatewayFailure('Gateway unavailable');
    }
  }
}

async function handleGatewayFailure(reason) {
  await disableProxy();
  isEnabled = false;
  await chrome.storage.local.set({
    isEnabled: false,
    gatewayStatus: { gateway_status: 'down' },
    lastError: `${reason}, proxy disabled`,
  });
  
  // Try to reconnect in 30 seconds
  chrome.alarms.create('reconnectRetry', { delayInMinutes: 0.5 });
}

async function refreshVpsStats() {
  try {
    const stats = await api.getVpsStats();
    await chrome.storage.local.set({ vpsStats: stats });

    if (isEnabled && currentProxy) {
      const currentVps = stats.find((s) => s.vps_id === currentProxy.vps_id);
      
      // If current VPS is offline, we MUST refresh regardless of manual selection
      if (currentVps && currentVps.status === 'offline') {
        console.warn('Current VPS went offline, refreshing proxy...');
        await requestNewProxy(null, true); // force refresh even if manual
      } else if (!manualVpsId) {
        // Only auto-refresh if not in manual mode
        // (Balancer might pick a better node based on load)
        // For now, we don't auto-switch if current is online to avoid connection drops
      }
    }
  } catch (error) {
    console.warn('Failed to fetch VPS stats:', error.message);
  }
}

async function requestNewProxy(country = null, force = false) {
  // If manual VPS is selected and not forced, don't request new one from balancer
  if (manualVpsId && !force) {
    return currentProxy;
  }

  try {
    await api.init();
    const proxy = await api.getProxy(country);
    currentProxy = proxy;
    manualVpsId = null; // Reset manual selection when requesting from balancer
    await chrome.storage.local.set({ 
      currentProxy: proxy, 
      manualVpsId: null,
      lastError: null 
    });
    if (isEnabled) {
      await applyProxy();
    }
    return proxy;
  } catch (error) {
    console.error('Failed to get proxy:', error.message);
    await chrome.storage.local.set({ lastError: error.message });
    throw error;
  }
}

function generatePacScript(proxyHost, proxyPort, domainRules) {
  const { domains, mode } = domainRules;

  // Format proxy host for IPv6
  const formattedHost = proxyHost.includes(':') ? `[${proxyHost}]` : proxyHost;

  if (!domains || domains.length === 0) {
    return `function FindProxyForURL(url, host) {
      return "PROXY ${formattedHost}:${proxyPort}";
    }`;
  }

  const conditions = domains
    .map((d) => {
      const domain = d.replace(/^\*\./, '');
      if (d.startsWith('*.')) {
        return `dnsDomainIs(host, "${domain}") || host === "${domain}"`;
      }
      return `host === "${d}" || dnsDomainIs(host, ".${d}")`;
    })
    .join(' || ');

  if (mode === 'whitelist') {
    return `function FindProxyForURL(url, host) {
      if (${conditions}) {
        return "PROXY ${formattedHost}:${proxyPort}";
      }
      return "DIRECT";
    }`;
  } else {
    return `function FindProxyForURL(url, host) {
      if (${conditions}) {
        return "DIRECT";
      }
      return "PROXY ${formattedHost}:${proxyPort}";
    }`;
  }
}

async function applyProxy() {
  if (!currentProxy) return;

  await chrome.proxy.settings.set({ value: { mode: 'system' }, scope: 'regular' });

  const data = await chrome.storage.local.get(['domainRules']);
  const domainRules = data.domainRules || { domains: [], mode: 'whitelist' };

  const pacScript = generatePacScript(
    currentProxy.proxy_host,
    currentProxy.proxy_port,
    domainRules
  );

  if (!domainRules.domains || domainRules.domains.length === 0) {
    const config = {
      mode: 'fixed_servers',
      rules: {
        singleProxy: {
          scheme: 'http',
          host: currentProxy.proxy_host,
          port: currentProxy.proxy_port,
        },
        bypassList: ['localhost', '127.0.0.1', '10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'],
      },
    };
    await chrome.proxy.settings.set({ value: config, scope: 'regular' });
  } else {
    const config = {
      mode: 'pac_script',
      pacScript: {
        data: pacScript,
      },
    };
    await chrome.proxy.settings.set({ value: config, scope: 'regular' });
  }

  try {
    await chrome.privacy.network.webRTCIPHandlingPolicy.set({
      value: 'disable_non_proxied_udp',
    });
  } catch (e) {
    console.warn('Could not set WebRTC policy:', e);
  }

  console.log('Proxy applied:', currentProxy.proxy_host, currentProxy.proxy_port);
}

async function disableProxy() {
  await chrome.proxy.settings.set({
    value: { mode: 'system' },
    scope: 'regular',
  });

  try {
    await chrome.privacy.network.webRTCIPHandlingPolicy.set({
      value: 'default',
    });
  } catch (e) {
    // ignore
  }

  console.log('Proxy disabled');
}

// Listen for messages from popup
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    try {
      await ensureInitialized();
      
      switch (message.action) {
        case 'enable': {
          await api.init();
          if (!currentProxy) {
            await requestNewProxy(message.country);
          }
          isEnabled = true;
          await applyProxy();
          await chrome.storage.local.set({ isEnabled: true });
          sendResponse({ success: true, proxy: currentProxy });
          break;
        }
        case 'disable': {
          isEnabled = false;
          await disableProxy();
          await chrome.storage.local.set({ isEnabled: false });
          sendResponse({ success: true });
          break;
        }
        case 'refreshProxy': {
          const proxy = await requestNewProxy(message.country, true); // force refresh
          if (isEnabled) {
            await applyProxy();
          }
          sendResponse({ success: true, proxy });
          break;
        }
        case 'selectVps': {
          const vps = message.vps;
          currentProxy = {
            proxy_host: vps.ip,
            proxy_port: vps.proxy_port,
            proxy_socks_port: vps.socks_port,
            proxy_username: vps.proxy_username || '',
            proxy_password: vps.proxy_password || '',
            vps_id: vps.vps_id,
            country: vps.country
          };
          manualVpsId = vps.vps_id;
          await chrome.storage.local.set({ 
            currentProxy, 
            manualVpsId,
            lastError: null 
          });
          if (isEnabled) {
            await applyProxy();
          }
          sendResponse({ success: true, proxy: currentProxy });
          break;
        }
        case 'updateDomainRules': {
          await chrome.storage.local.set({ domainRules: message.rules });
          try {
            await api.init();
            await api.saveDomainRules(message.rules.domains, message.rules.mode);
          } catch (e) {
            console.warn('Failed to sync domain rules to gateway:', e);
          }
          if (isEnabled && currentProxy) {
            await applyProxy();
          }
          sendResponse({ success: true });
          break;
        }
        case 'getState': {
          const data = await chrome.storage.local.get([
            'isEnabled', 'currentProxy', 'gatewayStatus', 'vpsStats',
            'domainRules', 'lastError', 'apiKey', 'clientId', 'manualVpsId'
          ]);
          sendResponse(data);
          break;
        }
        case 'setCredentials': {
          await api.setCredentials(message.apiKey, message.clientId);
          sendResponse({ success: true });
          break;
        }
        case 'refreshStatus': {
          await api.init();
          await refreshGatewayStatus();
          await refreshVpsStats();
          const data = await chrome.storage.local.get(['gatewayStatus', 'vpsStats']);
          sendResponse(data);
          break;
        }
        default:
          sendResponse({ error: 'Unknown action' });
      }
    } catch (error) {
      sendResponse({ error: error.message });
    }
  })();
  return true;
});
