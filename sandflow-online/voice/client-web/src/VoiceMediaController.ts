import type {
  VoiceConnectionState,
  VoiceControllerCallbacks,
  VoiceControllerErrorCode,
  VoiceCredentials,
  VoiceMembershipRequest,
  VoiceRemoteAudio,
  VoiceRoom,
  VoiceRoomFactory,
  VoiceTokenProvider,
} from './contracts.js';

export interface VoiceMediaControllerOptions {
  roomFactory: VoiceRoomFactory;
  tokenProvider: VoiceTokenProvider;
  callbacks?: VoiceControllerCallbacks;
  now?: () => number;
}

export class VoiceMediaController {
  readonly #roomFactory: VoiceRoomFactory;
  readonly #tokenProvider: VoiceTokenProvider;
  readonly #callbacks: VoiceControllerCallbacks;
  readonly #now: () => number;
  readonly #remoteMuted = new Set<string>();
  readonly #remoteBlocked = new Set<string>();
  readonly #remoteVolumes = new Map<string, number>();
  readonly #remoteTracks = new Map<string, VoiceRemoteAudio>();
  #state: VoiceConnectionState = 'idle';
  #room: VoiceRoom | undefined;
  #unsubscribers: Array<() => void> = [];
  #requestAbort: AbortController | undefined;
  #generation = 0;
  #microphoneConsented = false;
  #selfMuted = false;
  #pushToTalkPressed = false;
  #transmitting = false;
  readonly #microphoneQueues = new WeakMap<VoiceRoom, Promise<void>>();

  constructor(options: VoiceMediaControllerOptions) {
    this.#roomFactory = options.roomFactory;
    this.#tokenProvider = options.tokenProvider;
    this.#callbacks = options.callbacks ?? {};
    this.#now = options.now ?? Date.now;
  }

  get state(): VoiceConnectionState { return this.#state; }
  get microphoneConsented(): boolean { return this.#microphoneConsented; }
  get selfMuted(): boolean { return this.#selfMuted; }
  get transmitting(): boolean { return this.#transmitting; }

  /** Arms PTT. This method deliberately does not touch getUserMedia or publish a track. */
  enableMicrophone(): void {
    this.#microphoneConsented = true;
    this.#emitMicrophone();
  }

  async disableMicrophone(): Promise<void> {
    this.#microphoneConsented = false;
    this.#pushToTalkPressed = false;
    this.#transmitting = false;
    this.#emitMicrophone();
    this.#requestImmediateMicrophoneStop(true);
    void this.#syncMicrophone().catch(() => undefined);
  }

  async setSelfMuted(muted: boolean): Promise<void> {
    this.#selfMuted = muted;
    if (muted) {
      this.#transmitting = false;
      this.#emitMicrophone();
      this.#requestImmediateMicrophoneStop(false);
      void this.#syncMicrophone().catch(() => undefined);
      return;
    }
    await this.#syncMicrophone();
  }

  /** Call from pointer/key down and pointer/key up. First true after consent triggers browser mic permission. */
  async setPushToTalkPressed(pressed: boolean): Promise<void> {
    if (pressed && !this.#microphoneConsented) {
      this.#reportError('microphone-consent-required', new Error('Microphone consent is required before PTT.'));
      throw new Error('Microphone consent is required before PTT.');
    }
    this.#pushToTalkPressed = pressed;
    if (!pressed) {
      this.#transmitting = false;
      this.#emitMicrophone();
      this.#requestImmediateMicrophoneStop(false);
      void this.#syncMicrophone().catch(() => undefined);
      return;
    }
    await this.#syncMicrophone();
  }

  /** Must be called directly from a click/tap handler when autoplay is blocked. */
  async startAudioFromUserGesture(): Promise<void> {
    if (!this.#room) return;
    try { await this.#room.startAudio(); }
    catch (error) { this.#reportError('audio-playback-failed', error); throw error; }
  }

  async switchMembership(request: VoiceMembershipRequest): Promise<void> {
    if (!/^[0-9a-f]{32}$/.test(request.sandboxId)) {
      this.#reportError('invalid-membership', new Error('Invalid sandboxId.'));
      throw new TypeError('Invalid sandboxId.');
    }

    const generation = ++this.#generation;
    this.#requestAbort?.abort();
    this.#requestAbort = new AbortController();
    this.#pushToTalkPressed = false;
    await this.#teardownRoom();
    if (generation !== this.#generation) return;
    this.#setState('connecting');

    let credentials: VoiceCredentials;
    try {
      credentials = await this.#tokenProvider.getToken(request, this.#requestAbort.signal);
    } catch (error) {
      if (generation !== this.#generation || this.#requestAbort.signal.aborted) return;
      this.#setState('error');
      this.#reportError('token-request-failed', error);
      throw error;
    }
    if (generation !== this.#generation) return;

    try { this.#validateCredentials(credentials, request); }
    catch (error) {
      this.#setState('error');
      this.#reportError('invalid-credentials', error);
      throw error;
    }

    const room = this.#roomFactory.create();
    this.#room = room;
    this.#bindRoom(room, generation);
    try {
      await room.connect(credentials.serverUrl, credentials.token);
      if (generation !== this.#generation || this.#room !== room) {
        await room.stopMicrophone().catch(() => undefined);
        await room.disconnect().catch(() => undefined);
        return;
      }
      await room.setMicrophoneEnabled(false);
      this.#transmitting = false;
      this.#setState('connected');
      this.#emitMicrophone();
      await this.#syncMicrophone();
    } catch (error) {
      if (this.#room === room) await this.#teardownRoom();
      if (generation === this.#generation) {
        this.#setState('error');
        this.#reportError('connect-failed', error);
      }
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    ++this.#generation;
    this.#requestAbort?.abort();
    this.#requestAbort = undefined;
    this.#pushToTalkPressed = false;
    this.#setState('disconnecting');
    await this.#teardownRoom();
    this.#setState('idle');
  }

  setRemoteMuted(participantId: string, muted: boolean): void {
    this.#validateParticipantId(participantId);
    if (muted) this.#remoteMuted.add(participantId); else this.#remoteMuted.delete(participantId);
    this.#applyRemotePreferences(participantId);
  }

  setRemoteVolume(participantId: string, volume: number): void {
    this.#validateParticipantId(participantId);
    if (!Number.isFinite(volume) || volume < 0 || volume > 1) throw new RangeError('Remote volume must be between 0 and 1.');
    this.#remoteVolumes.set(participantId, volume);
    this.#applyRemotePreferences(participantId);
  }

  setRemoteBlocked(participantId: string, blocked: boolean): void {
    this.#validateParticipantId(participantId);
    if (blocked) this.#remoteBlocked.add(participantId); else this.#remoteBlocked.delete(participantId);
    this.#applyRemotePreferences(participantId);
  }

  #bindRoom(room: VoiceRoom, generation: number): void {
    this.#unsubscribers = [
      room.on('trackSubscribed', (audio) => {
        if (generation !== this.#generation || this.#room !== room) { audio.detach(); return; }
        this.#remoteTracks.set(audio.trackId, audio);
        this.#applyTrackPreferences(audio);
        this.#callbacks.onRemoteAudioChanged?.(audio.participantId);
      }),
      room.on('trackUnsubscribed', (audio) => {
        audio.detach();
        this.#remoteTracks.delete(audio.trackId);
        this.#callbacks.onRemoteAudioChanged?.(audio.participantId);
      }),
      room.on('activeSpeakersChanged', (ids) => {
        if (generation !== this.#generation) return;
        this.#callbacks.onActiveSpeakersChanged?.(ids.filter((id) => !this.#remoteBlocked.has(id)));
      }),
      room.on('reconnecting', () => { if (generation === this.#generation) this.#setState('reconnecting'); }),
      room.on('reconnected', () => {
        if (generation !== this.#generation) return;
        this.#setState('connected');
        void this.#syncMicrophone().catch(() => undefined);
      }),
      room.on('disconnected', () => {
        if (generation !== this.#generation || this.#room !== room) return;
        this.#clearRemoteTracks();
        this.#transmitting = false;
        this.#setState('idle');
        this.#emitMicrophone();
      }),
      room.on('mediaDeviceError', (error) => {
        if (generation !== this.#generation) return;
        this.#pushToTalkPressed = false;
        this.#reportError('microphone-failed', error);
        void this.#syncMicrophone().catch(() => undefined);
      }),
      room.on('audioPlaybackBlocked', () => {
        if (generation === this.#generation) this.#callbacks.onAudioPlaybackBlocked?.();
      }),
    ];
  }

  #syncMicrophone(): Promise<void> {
    const room = this.#room;
    const generation = this.#generation;
    if (!room) {
      this.#transmitting = false;
      this.#emitMicrophone();
      return Promise.resolve();
    }
    const previous = this.#microphoneQueues.get(room) ?? Promise.resolve();
    const operation = previous.catch(() => undefined).then(async () => {
      if (this.#room !== room || this.#state !== 'connected') {
        this.#transmitting = false;
        this.#emitMicrophone();
        return;
      }
      const shouldTransmit = this.#microphoneConsented && !this.#selfMuted && this.#pushToTalkPressed;
      try {
        await room.setMicrophoneEnabled(shouldTransmit);
        if (generation !== this.#generation || this.#room !== room) {
          if (shouldTransmit) void room.setMicrophoneEnabled(false).catch(() => undefined);
          return;
        }
        const latestIntentAllowsTransmission =
          this.#microphoneConsented && !this.#selfMuted && this.#pushToTalkPressed;
        if (!latestIntentAllowsTransmission) {
          this.#transmitting = false;
          this.#emitMicrophone();
          void room.stopMicrophone().catch(() => undefined);
          return;
        }
        this.#transmitting = shouldTransmit && latestIntentAllowsTransmission;
        this.#emitMicrophone();
      } catch (error) {
        if (generation === this.#generation) {
          this.#pushToTalkPressed = false;
          this.#transmitting = false;
          this.#emitMicrophone();
          this.#reportError('microphone-failed', error);
        }
        throw error;
      }
    });
    this.#microphoneQueues.set(room, operation);
    return operation;
  }

  async #teardownRoom(): Promise<void> {
    const room = this.#room;
    this.#room = undefined;
    for (const unsubscribe of this.#unsubscribers.splice(0)) unsubscribe();
    this.#clearRemoteTracks();
    if (!room) return;
    // Do not wait for an unresolved getUserMedia prompt. Disconnect owns immediate
    // teardown; a late enable completion observes stale generation/room and schedules stop.
    void room.stopMicrophone().catch(() => undefined);
    await room.disconnect().catch(() => undefined);
    this.#transmitting = false;
    this.#emitMicrophone();
  }

  #applyRemotePreferences(participantId: string): void {
    for (const audio of this.#remoteTracks.values()) {
      if (audio.participantId === participantId) this.#applyTrackPreferences(audio);
    }
    this.#callbacks.onRemoteAudioChanged?.(participantId);
  }

  #requestImmediateMicrophoneStop(fullStop: boolean): void {
    const room = this.#room;
    if (!room) return;
    const operation = fullStop ? room.stopMicrophone() : room.setMicrophoneEnabled(false);
    void operation.catch((error) => {
      if (this.#room === room) this.#reportError('microphone-failed', error);
    });
  }

  #applyTrackPreferences(audio: VoiceRemoteAudio): void {
    if (this.#remoteBlocked.has(audio.participantId)) {
      audio.setVolume(0);
      audio.detach();
      return;
    }
    audio.attach();
    const volume = this.#remoteMuted.has(audio.participantId) ? 0 : (this.#remoteVolumes.get(audio.participantId) ?? 1);
    audio.setVolume(volume);
  }

  #clearRemoteTracks(): void {
    for (const audio of this.#remoteTracks.values()) audio.detach();
    this.#remoteTracks.clear();
  }

  #validateCredentials(credentials: VoiceCredentials, request: VoiceMembershipRequest): void {
    if (credentials.serverUrl !== 'wss://autorig.online/sandflow/voice') throw new TypeError('Voice server URL is not the pinned SandFlow endpoint.');
    if (!credentials.token || credentials.token.length > 16_384) throw new TypeError('Voice token is missing or oversized.');
    if (!/^sf:[0-9a-f]{32}:(all|team:[01])$/.test(credentials.roomName)) throw new TypeError('Voice room name is invalid.');
    if (!credentials.roomName.startsWith(`sf:${request.sandboxId}:`)) throw new TypeError('Voice room does not match the requested sandbox.');
    this.#validateParticipantId(credentials.participantId);
    const expiry = Date.parse(credentials.expiresAt);
    if (!Number.isFinite(expiry) || expiry <= this.#now()) throw new TypeError('Voice token is already expired.');
  }

  #validateParticipantId(participantId: string): void {
    if (!/^[A-Za-z0-9][A-Za-z0-9_.:@-]{2,127}$/.test(participantId)) throw new TypeError('Participant ID is invalid.');
  }

  #setState(state: VoiceConnectionState): void {
    if (this.#state === state) return;
    this.#state = state;
    this.#callbacks.onStateChanged?.(state);
  }

  #emitMicrophone(): void {
    this.#callbacks.onMicrophoneChanged?.({
      consented: this.#microphoneConsented,
      selfMuted: this.#selfMuted,
      transmitting: this.#transmitting,
    });
  }

  #reportError(code: VoiceControllerErrorCode, error: unknown): void {
    this.#callbacks.onError?.(code, error);
  }
}
