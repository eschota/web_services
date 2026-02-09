/**
 * QwertyStock VPN - Popup Logic
 */

document.addEventListener('DOMContentLoaded', async () => {
  // Elements
  const proxyToggle = document.getElementById('proxyToggle');
  const toggleStatus = document.getElementById('toggleStatus');
  const proxyInfo = document.getElementById('proxyInfo');
  const statusDot = document.getElementById('statusDot');
  const statusText = document.getElementById('statusText');
  const countrySelect = document.getElementById('countrySelect');
  const refreshProxy = document.getElementById('refreshProxy');
  const errorBanner = document.getElementById('errorBanner');
  const errorText = document.getElementById('errorText');
  const dismissError = document.getElementById('dismissError');
  const totalVps = document.getElementById('totalVps');
  const onlineVps = document.getElementById('onlineVps');
  const activeProxies = document.getElementById('activeProxies');
  const avgTraffic = document.getElementById('avgTraffic');
  const vpsTableBody = document.getElementById('vpsTableBody');
  const domainInput = document.getElementById('domainInput');
  const addDomainBtn = document.getElementById('addDomain');
  const domainList = document.getElementById('domainList');
  const syncDomains = document.getElementById('syncDomains');
  const apiKeyInput = document.getElementById('apiKeyInput');
  const clientIdInput = document.getElementById('clientIdInput');
  const saveSettings = document.getElementById('saveSettings');
  const refreshAll = document.getElementById('refreshAll');

  let state = {};
  let localDomains = [];
  let localMode = 'whitelist';

  // Tab switching
  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      tab.classList.add('active');
      document.getElementById(`tab-${tab.dataset.tab}`).classList.add('active');
    });
  });

  // Load state
  async function loadState() {
    return new Promise((resolve) => {
      chrome.runtime.sendMessage({ action: 'getState' }, (response) => {
        state = response || {};
        resolve(state);
      });
    });
  }

  // Update UI
  function updateUI() {
    // Toggle
    const isEnabled = state.isEnabled || false;
    proxyToggle.checked = isEnabled;
    toggleStatus.textContent = isEnabled ? 'VPN ON' : 'VPN OFF';
    toggleStatus.style.color = isEnabled ? 'var(--success)' : 'var(--text-primary)';

    // Proxy info
    const proxy = state.currentProxy;
    if (proxy && isEnabled) {
      proxyInfo.textContent = `${proxy.proxy_host}:${proxy.proxy_port} (${proxy.country})`;
    } else if (proxy) {
      proxyInfo.textContent = `Ready: ${proxy.proxy_host}:${proxy.proxy_port}`;
    } else {
      proxyInfo.textContent = 'Not connected';
    }

    // Gateway status
    const gw = state.gatewayStatus;
    if (gw) {
      const gwStatus = gw.gateway_status || 'down';
      statusDot.className = `status-dot ${gwStatus === 'up' ? 'online' : 'offline'}`;
      statusText.textContent = gwStatus === 'up' ? 'Online' : 'Offline';

      totalVps.textContent = gw.total_vps_count || 0;
      onlineVps.textContent = gw.online_vps_count || 0;
      activeProxies.textContent = gw.total_active_proxies || 0;
      avgTraffic.textContent = (gw.average_traffic_gbps_total || 0).toFixed(4);
    }

    // Error
    if (state.lastError) {
      errorBanner.classList.remove('hidden');
      errorText.textContent = state.lastError;
    } else {
      errorBanner.classList.add('hidden');
    }

    // VPS Stats
    const stats = state.vpsStats || [];
    const currentVpsId = state.currentProxy ? state.currentProxy.vps_id : null;
    
    if (stats.length > 0) {
      vpsTableBody.innerHTML = stats.map(vps => {
        const isActive = vps.vps_id === currentVpsId;
        const isOnline = vps.status === 'online';
        
        return `
          <tr class="${isActive ? 'active' : ''}">
            <td>${vps.vps_id}</td>
            <td>${vps.country}</td>
            <td><span class="badge badge-${vps.status}">${vps.status}</span></td>
            <td>${vps.active_connections}</td>
            <td>${(vps.traffic_gbps_avg || 0).toFixed(4)}</td>
            <td>
              ${isActive 
                ? '<span class="badge badge-active">Active</span>' 
                : `<button class="btn btn-sm btn-primary" data-vps-id="${vps.vps_id}" ${!isOnline ? 'disabled' : ''}>Use</button>`
              }
            </td>
          </tr>
        `;
      }).join('');

      // Bind Use buttons
      vpsTableBody.querySelectorAll('button[data-vps-id]').forEach(btn => {
        btn.addEventListener('click', () => {
          const vpsId = parseInt(btn.dataset.vpsId);
          const vps = stats.find(s => s.vps_id === vpsId);
          if (vps) {
            chrome.runtime.sendMessage({ action: 'selectVps', vps }, async (response) => {
              if (response?.error) {
                showError(response.error);
              }
              await loadState();
              updateUI();
            });
          }
        });
      });
    } else {
      vpsTableBody.innerHTML = '<tr class="empty-row"><td colspan="6">No VPS data</td></tr>';
    }

    // Domain rules
    const rules = state.domainRules || { domains: [], mode: 'whitelist' };
    localDomains = [...(rules.domains || [])];
    localMode = rules.mode || 'whitelist';
    document.querySelector(`input[name="domainMode"][value="${localMode}"]`).checked = true;
    renderDomains();

    // Settings
    apiKeyInput.value = state.apiKey || '';
    clientIdInput.value = state.clientId || '';
  }

  function renderDomains() {
    if (localDomains.length === 0) {
      domainList.innerHTML = '<div class="empty-domains">No domains added yet</div>';
      return;
    }

    domainList.innerHTML = localDomains.map((domain, i) => `
      <div class="domain-item">
        <span class="domain-name">${escapeHtml(domain)}</span>
        <div class="domain-actions">
          <button class="btn btn-sm btn-danger" data-remove="${i}" title="Remove">&times;</button>
        </div>
      </div>
    `).join('');

    // Bind remove buttons
    domainList.querySelectorAll('[data-remove]').forEach(btn => {
      btn.addEventListener('click', () => {
        const idx = parseInt(btn.dataset.remove);
        localDomains.splice(idx, 1);
        renderDomains();
        saveDomainRulesLocal();
      });
    });
  }

  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  function saveDomainRulesLocal() {
    const rules = { domains: localDomains, mode: localMode };
    chrome.runtime.sendMessage({ action: 'updateDomainRules', rules });
  }

  // Event handlers
  proxyToggle.addEventListener('change', () => {
    const action = proxyToggle.checked ? 'enable' : 'disable';
    const country = countrySelect.value || null;
    chrome.runtime.sendMessage({ action, country }, async (response) => {
      if (response?.error) {
        proxyToggle.checked = !proxyToggle.checked;
        showError(response.error);
      }
      await loadState();
      updateUI();
    });
  });

  refreshProxy.addEventListener('click', () => {
    const country = countrySelect.value || null;
    refreshProxy.disabled = true;
    refreshProxy.textContent = '...';
    chrome.runtime.sendMessage({ action: 'refreshProxy', country }, async (response) => {
      refreshProxy.disabled = false;
      refreshProxy.innerHTML = '&#8635; Refresh';
      if (response?.error) {
        showError(response.error);
      }
      await loadState();
      updateUI();
    });
  });

  dismissError.addEventListener('click', () => {
    errorBanner.classList.add('hidden');
    chrome.storage.local.set({ lastError: null });
  });

  addDomainBtn.addEventListener('click', () => {
    const domain = domainInput.value.trim().toLowerCase();
    if (!domain) return;
    if (localDomains.includes(domain)) {
      showError('Domain already exists');
      return;
    }
    localDomains.push(domain);
    domainInput.value = '';
    renderDomains();
    saveDomainRulesLocal();
  });

  domainInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') addDomainBtn.click();
  });

  document.querySelectorAll('input[name="domainMode"]').forEach(radio => {
    radio.addEventListener('change', () => {
      localMode = radio.value;
      saveDomainRulesLocal();
    });
  });

  syncDomains.addEventListener('click', async () => {
    syncDomains.disabled = true;
    syncDomains.textContent = 'Syncing...';
    saveDomainRulesLocal();
    setTimeout(() => {
      syncDomains.disabled = false;
      syncDomains.textContent = 'Sync with Gateway';
    }, 1000);
  });

  saveSettings.addEventListener('click', () => {
    const apiKey = apiKeyInput.value.trim();
    const clientId = clientIdInput.value.trim();
    if (!apiKey || !clientId) {
      showError('API Key and Client ID are required');
      return;
    }
    chrome.runtime.sendMessage({ action: 'setCredentials', apiKey, clientId }, () => {
      saveSettings.textContent = 'Saved!';
      setTimeout(() => { saveSettings.textContent = 'Save Settings'; }, 1500);
    });
  });

  refreshAll.addEventListener('click', async () => {
    refreshAll.textContent = 'Refreshing...';
    chrome.runtime.sendMessage({ action: 'refreshStatus' }, async () => {
      await loadState();
      updateUI();
      refreshAll.textContent = 'Refresh All';
    });
  });

  function showError(msg) {
    errorBanner.classList.remove('hidden');
    errorText.textContent = msg;
    setTimeout(() => errorBanner.classList.add('hidden'), 5000);
  }

  // Initial load
  await loadState();
  updateUI();

  // Refresh status immediately
  chrome.runtime.sendMessage({ action: 'refreshStatus' }, async () => {
    await loadState();
    updateUI();
  });
});
