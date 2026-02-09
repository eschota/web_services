/**
 * QwertyStock VPN - Gateway API client
 */

const GATEWAY_BASE_URL = 'https://autorig.online/vpn';

class GatewayAPI {
  constructor() {
    this.baseUrl = GATEWAY_BASE_URL;
    this.apiKey = '';
    this.clientId = '';
  }

  async init() {
    const data = await chrome.storage.local.get(['apiKey', 'clientId']);
    this.apiKey = data.apiKey || '';
    this.clientId = data.clientId || '';
  }

  async setCredentials(apiKey, clientId) {
    this.apiKey = apiKey;
    this.clientId = clientId;
    await chrome.storage.local.set({ apiKey, clientId });
  }

  async request(endpoint, options = {}) {
    const url = `${this.baseUrl}${endpoint}`;
    const headers = {
      'Content-Type': 'application/json',
      ...(this.apiKey ? { 'X-API-Key': this.apiKey } : {}),
    };

    try {
      const response = await fetch(url, {
        ...options,
        headers: { ...headers, ...options.headers },
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `HTTP ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      if (error.message === 'Failed to fetch') {
        throw new Error('Gateway unavailable');
      }
      throw error;
    }
  }

  async getProxy(country = null) {
    let endpoint = `/api/get-proxy?client_id=${encodeURIComponent(this.clientId)}`;
    if (country) {
      endpoint += `&country=${encodeURIComponent(country)}`;
    }
    return this.request(endpoint);
  }

  async getStatus() {
    return this.request('/api/status');
  }

  async getVpsStats() {
    return this.request('/api/vps-stats');
  }

  async getDomainRules() {
    return this.request(`/api/domain-rules?client_id=${encodeURIComponent(this.clientId)}`);
  }

  async saveDomainRules(domains, mode) {
    return this.request('/api/domain-rules', {
      method: 'POST',
      body: JSON.stringify({
        client_id: this.clientId,
        domains,
        mode,
      }),
    });
  }
}

// Export for use in service worker and popup
if (typeof globalThis !== 'undefined') {
  globalThis.GatewayAPI = GatewayAPI;
}
