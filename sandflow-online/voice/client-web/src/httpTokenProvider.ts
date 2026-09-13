import type { VoiceCredentials, VoiceMembershipRequest, VoiceTokenProvider } from './contracts.js';

export interface HttpVoiceTokenProviderOptions {
  /** Same-origin API prefix, normally /sandflow/api/v1. */
  apiBaseUrl: string;
  /** Optional bearer session. Omit to use the HttpOnly same-origin sf_session cookie. */
  getBearerSession?: () => string | undefined;
  fetch?: typeof globalThis.fetch;
}

export class HttpVoiceTokenProvider implements VoiceTokenProvider {
  readonly #baseUrl: string;
  readonly #getBearerSession: (() => string | undefined) | undefined;
  readonly #fetch: typeof globalThis.fetch;

  constructor(options: HttpVoiceTokenProviderOptions) {
    if (options.apiBaseUrl !== '/sandflow/api/v1') {
      throw new TypeError('apiBaseUrl must be exactly /sandflow/api/v1.');
    }
    this.#baseUrl = options.apiBaseUrl;
    this.#getBearerSession = options.getBearerSession;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  async getToken(request: VoiceMembershipRequest, signal: AbortSignal): Promise<VoiceCredentials> {
    if (!/^[0-9a-f]{32}$/.test(request.sandboxId)) {
      throw new TypeError('sandboxId must be a lowercase GUID in N format.');
    }
    const headers = new Headers({ Accept: 'application/json' });
    const bearer = this.#getBearerSession?.();
    if (bearer) {
      if (!/^[0-9A-Fa-f]{64}$/.test(bearer)) throw new TypeError('Bearer session is invalid.');
      headers.set('Authorization', `Bearer ${bearer}`);
    }

    const response = await this.#fetch(
      `${this.#baseUrl}/worlds/${encodeURIComponent(request.sandboxId)}/voice-token`,
      {
        method: 'POST',
        credentials: 'same-origin',
        cache: 'no-store',
        redirect: 'error',
        headers,
        signal,
      },
    );
    if (!response.ok) throw new Error(`Voice token request failed with HTTP ${response.status}.`);
    return (await response.json()) as VoiceCredentials;
  }
}
