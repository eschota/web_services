import {
  Room,
  RoomEvent,
  Track,
  createLocalAudioTrack,
  type AudioCaptureOptions,
  type LocalAudioTrack,
  type LocalTrack,
  type Participant,
  type RemoteAudioTrack,
  type RemoteParticipant,
  type RemoteTrack,
  type RemoteTrackPublication,
  type TrackPublishDefaults,
} from 'livekit-client';
import type { VoiceRemoteAudio, VoiceRoom, VoiceRoomEvents, VoiceRoomFactory } from './contracts.js';
import { MicrophonePublicationGate } from './microphonePublicationGate.js';

export const SANDFLOW_AUDIO_CAPTURE_OPTIONS = {
  channelCount: { exact: 1 },
  autoGainControl: true,
  echoCancellation: true,
  noiseSuppression: true,
} as const satisfies AudioCaptureOptions;

export const SANDFLOW_AUDIO_PUBLISH_DEFAULTS = {
  audioPreset: { maxBitrate: 32_000 },
  dtx: true,
  red: true,
  forceStereo: false,
  preConnectBuffer: false,
} as const satisfies TrackPublishDefaults;

class LiveKitRemoteAudio implements VoiceRemoteAudio {
  readonly participantId: string;
  readonly trackId: string;
  readonly #track: RemoteAudioTrack;
  readonly #sink: HTMLElement;
  #elements: HTMLMediaElement[] = [];
  #volume = 1;

  constructor(track: RemoteAudioTrack, participantId: string, trackId: string, sink: HTMLElement) {
    this.#track = track;
    this.participantId = participantId;
    this.trackId = trackId;
    this.#sink = sink;
  }

  attach(): void {
    if (this.#elements.length > 0) return;
    const element = this.#track.attach();
    if (!(element instanceof HTMLMediaElement)) throw new Error('LiveKit audio track returned a non-media element.');
    element.autoplay = true;
    element.volume = this.#volume;
    element.dataset.sandflowVoiceParticipant = this.participantId;
    element.hidden = true;
    this.#sink.appendChild(element);
    this.#elements = [element];
  }

  detach(): void {
    for (const element of this.#track.detach()) element.remove();
    this.#elements = [];
  }

  setVolume(volume: number): void {
    this.#volume = volume;
    this.#track.setVolume(volume);
    for (const element of this.#elements) element.volume = volume;
  }
}

class LiveKitBrowserRoom implements VoiceRoom {
  readonly #room: Room;
  readonly #sink: HTMLElement;
  readonly #audioByTrackId = new Map<string, LiveKitRemoteAudio>();
  readonly #microphoneGate = new MicrophonePublicationGate<LocalAudioTrack>();

  constructor(sink: HTMLElement) {
    this.#sink = sink;
    this.#room = new Room({
      adaptiveStream: false,
      dynacast: false,
      audioCaptureDefaults: SANDFLOW_AUDIO_CAPTURE_OPTIONS,
      publishDefaults: SANDFLOW_AUDIO_PUBLISH_DEFAULTS,
    });
  }

  connect(serverUrl: string, token: string): Promise<void> {
    return this.#room.connect(serverUrl, token, { autoSubscribe: true });
  }

  async disconnect(): Promise<void> {
    this.#microphoneGate.invalidate();
    for (const audio of this.#audioByTrackId.values()) audio.detach();
    this.#audioByTrackId.clear();
    await this.#room.disconnect();
  }

  async setMicrophoneEnabled(enabled: boolean): Promise<void> {
    if (!enabled) {
      await this.stopMicrophone();
      return;
    }

    await this.#microphoneGate.captureThenPublish(
      () => Boolean(this.#room.localParticipant.getTrackPublication(Track.Source.Microphone)?.track),
      () => createLocalAudioTrack(SANDFLOW_AUDIO_CAPTURE_OPTIONS),
      async (track) => {
        // LiveKit 2.21.0 has an exactOptionalPropertyTypes declaration mismatch between
        // LocalAudioTrack and LocalTrack. This type-only cast preserves the original
        // LocalAudioTrack at runtime, avoiding a second wrapper around its MediaStreamTrack.
        await this.#room.localParticipant.publishTrack(track as unknown as LocalTrack, {
          ...SANDFLOW_AUDIO_PUBLISH_DEFAULTS,
          source: Track.Source.Microphone,
        });
      },
      () => this.#stopPublishedMicrophone(),
    );
  }

  async stopMicrophone(): Promise<void> {
    this.#microphoneGate.invalidate();
    await this.#stopPublishedMicrophone();
  }

  async #stopPublishedMicrophone(): Promise<void> {
    const publication = this.#room.localParticipant.getTrackPublication(Track.Source.Microphone);
    if (publication?.track) {
      await this.#room.localParticipant.unpublishTrack(publication.track.mediaStreamTrack, true);
    }
  }

  startAudio(): Promise<void> {
    return this.#room.startAudio();
  }

  on<K extends keyof VoiceRoomEvents>(event: K, listener: VoiceRoomEvents[K]): () => void {
    const bindings: Record<keyof VoiceRoomEvents, { sdkEvent: RoomEvent; handler: (...args: never[]) => void }> = {
      trackSubscribed: {
        sdkEvent: RoomEvent.TrackSubscribed,
        handler: ((track: RemoteTrack, publication: RemoteTrackPublication, participant: RemoteParticipant) => {
          if (track.kind !== Track.Kind.Audio) return;
          const audio = new LiveKitRemoteAudio(track as RemoteAudioTrack, participant.identity, publication.trackSid, this.#sink);
          this.#audioByTrackId.set(publication.trackSid, audio);
          (listener as VoiceRoomEvents['trackSubscribed'])(audio);
        }) as (...args: never[]) => void,
      },
      trackUnsubscribed: {
        sdkEvent: RoomEvent.TrackUnsubscribed,
        handler: ((track: RemoteTrack, publication: RemoteTrackPublication) => {
          if (track.kind !== Track.Kind.Audio) return;
          const audio = this.#audioByTrackId.get(publication.trackSid);
          if (!audio) return;
          this.#audioByTrackId.delete(publication.trackSid);
          (listener as VoiceRoomEvents['trackUnsubscribed'])(audio);
        }) as (...args: never[]) => void,
      },
      activeSpeakersChanged: {
        sdkEvent: RoomEvent.ActiveSpeakersChanged,
        handler: ((speakers: Participant[]) => {
          (listener as VoiceRoomEvents['activeSpeakersChanged'])(speakers.map((speaker) => speaker.identity));
        }) as (...args: never[]) => void,
      },
      reconnecting: { sdkEvent: RoomEvent.Reconnecting, handler: listener as (...args: never[]) => void },
      reconnected: { sdkEvent: RoomEvent.Reconnected, handler: listener as (...args: never[]) => void },
      disconnected: { sdkEvent: RoomEvent.Disconnected, handler: listener as (...args: never[]) => void },
      mediaDeviceError: { sdkEvent: RoomEvent.MediaDevicesError, handler: listener as (...args: never[]) => void },
      audioPlaybackBlocked: {
        sdkEvent: RoomEvent.AudioPlaybackStatusChanged,
        handler: (() => {
          if (!this.#room.canPlaybackAudio) (listener as VoiceRoomEvents['audioPlaybackBlocked'])();
        }) as (...args: never[]) => void,
      },
    };
    const binding = bindings[event];
    this.#room.on(binding.sdkEvent, binding.handler);
    return () => this.#room.off(binding.sdkEvent, binding.handler);
  }
}

export class LiveKitBrowserRoomFactory implements VoiceRoomFactory {
  readonly #audioSink: HTMLElement;

  constructor(audioSink: HTMLElement) {
    this.#audioSink = audioSink;
  }

  create(): VoiceRoom {
    return new LiveKitBrowserRoom(this.#audioSink);
  }
}
