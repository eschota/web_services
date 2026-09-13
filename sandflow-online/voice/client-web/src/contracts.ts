export type VoiceConnectionState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'disconnecting'
  | 'error';

export interface VoiceMembershipRequest {
  /** Trusted backend world ID: lowercase GUID in N format, already joined by this session. */
  sandboxId: string;
}

export interface VoiceCredentials {
  serverUrl: string;
  token: string;
  roomName: string;
  participantId: string;
  expiresAt: string;
}

export interface VoiceTokenProvider {
  getToken(request: VoiceMembershipRequest, signal: AbortSignal): Promise<VoiceCredentials>;
}

export interface VoiceRemoteAudio {
  readonly participantId: string;
  readonly trackId: string;
  attach(): void;
  detach(): void;
  setVolume(volume: number): void;
}

export interface VoiceRoomEvents {
  trackSubscribed: (audio: VoiceRemoteAudio) => void;
  trackUnsubscribed: (audio: VoiceRemoteAudio) => void;
  activeSpeakersChanged: (participantIds: readonly string[]) => void;
  reconnecting: () => void;
  reconnected: () => void;
  disconnected: () => void;
  mediaDeviceError: (error: unknown) => void;
  audioPlaybackBlocked: () => void;
}

export interface VoiceRoom {
  connect(serverUrl: string, token: string): Promise<void>;
  disconnect(): Promise<void>;
  setMicrophoneEnabled(enabled: boolean): Promise<void>;
  /** Fully unpublishes and stops the local microphone for consent revocation/room teardown. */
  stopMicrophone(): Promise<void>;
  startAudio(): Promise<void>;
  on<K extends keyof VoiceRoomEvents>(event: K, listener: VoiceRoomEvents[K]): () => void;
}

export interface VoiceRoomFactory {
  create(): VoiceRoom;
}

export interface VoiceControllerCallbacks {
  onStateChanged?(state: VoiceConnectionState): void;
  onMicrophoneChanged?(state: { consented: boolean; selfMuted: boolean; transmitting: boolean }): void;
  onRemoteAudioChanged?(participantId: string): void;
  onActiveSpeakersChanged?(participantIds: readonly string[]): void;
  onAudioPlaybackBlocked?(): void;
  onError?(code: VoiceControllerErrorCode, error: unknown): void;
}

export type VoiceControllerErrorCode =
  | 'invalid-membership'
  | 'invalid-credentials'
  | 'token-request-failed'
  | 'connect-failed'
  | 'microphone-consent-required'
  | 'microphone-failed'
  | 'audio-playback-failed';
