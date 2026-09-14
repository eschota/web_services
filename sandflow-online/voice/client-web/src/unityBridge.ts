import { HttpVoiceTokenProvider } from './httpTokenProvider.js';
import { LiveKitBrowserRoomFactory } from './liveKitBrowserAdapter.js';
import { VoiceMediaController } from './VoiceMediaController.js';
import type { VoiceControllerCallbacks } from './contracts.js';

const ProductionOrigin = 'https://autorig.online';

export interface UnityVoiceControllerPort {
  switchMembership(request: { sandboxId: string }): Promise<void>;
  enableMicrophone(): void;
  disableMicrophone(): Promise<void>;
  setPushToTalkPressed(pressed: boolean): Promise<void>;
  setSelfMuted(muted: boolean): Promise<void>;
  setRemoteMuted(participantId: string, muted: boolean): void;
  setRemoteVolume(participantId: string, volume: number): void;
  setRemoteBlocked(participantId: string, blocked: boolean): void;
  startAudioFromUserGesture(): Promise<void>;
  disconnect(): Promise<void>;
}

export interface UnityVoiceBridgeDependencies {
  origin: string;
  sendMessage(gameObjectName: string, methodName: string, payload: string): void;
  createController(callbacks: VoiceControllerCallbacks): UnityVoiceControllerPort;
}

export class UnityVoiceBridge {
  readonly #dependencies: UnityVoiceBridgeDependencies;
  #gameObjectName = '';
  #controller: UnityVoiceControllerPort | undefined;

  constructor(dependencies: UnityVoiceBridgeDependencies) {
    this.#dependencies = dependencies;
  }

  initialize(gameObjectName: string, serviceEnabled: boolean): boolean {
    this.#gameObjectName = gameObjectName;
    if (!serviceEnabled || this.#dependencies.origin !== ProductionOrigin) {
      this.#send('OnSandFlowVoiceState', 'disabled');
      return false;
    }
    if (this.#controller) return true;

    this.#controller = this.#dependencies.createController({
      onStateChanged: (state) => this.#send('OnSandFlowVoiceState', state),
      onMicrophoneChanged: (state) => this.#send('OnSandFlowVoiceMicrophone', JSON.stringify(state)),
      onActiveSpeakersChanged: (participantIds) =>
        this.#send('OnSandFlowVoiceActiveSpeakers', JSON.stringify({ participantIds })),
      onAudioPlaybackBlocked: () => this.#send('OnSandFlowVoiceAudioBlocked', ''),
      onError: (code) => this.#send('OnSandFlowVoiceError', code),
    });
    return true;
  }

  connect(sandboxId: string): void {
    void this.#controller?.switchMembership({ sandboxId }).catch(() => undefined);
  }

  enableMicrophone(): void { this.#controller?.enableMicrophone(); }
  disableMicrophone(): void { void this.#controller?.disableMicrophone().catch(() => undefined); }
  setPushToTalkPressed(pressed: boolean): void {
    void this.#controller?.setPushToTalkPressed(pressed).catch(() => undefined);
  }
  setSelfMuted(muted: boolean): void { void this.#controller?.setSelfMuted(muted).catch(() => undefined); }
  setRemoteMuted(participantId: string, muted: boolean): void {
    this.#controller?.setRemoteMuted(participantId, muted);
  }
  setRemoteVolume(participantId: string, volume: number): void {
    this.#controller?.setRemoteVolume(participantId, volume);
  }
  setRemoteBlocked(participantId: string, blocked: boolean): void {
    this.#controller?.setRemoteBlocked(participantId, blocked);
  }
  startAudioFromUserGesture(): void {
    void this.#controller?.startAudioFromUserGesture().catch(() => undefined);
  }
  disconnect(): void { void this.#controller?.disconnect().catch(() => undefined); }

  #send(methodName: string, payload: string): void {
    if (this.#gameObjectName) this.#dependencies.sendMessage(this.#gameObjectName, methodName, payload);
  }
}

declare global {
  interface Window {
    Module?: { SendMessage?: (gameObjectName: string, methodName: string, payload: string) => void };
  }
}

function createProductionBridge(): UnityVoiceBridge {
  let audioSink: HTMLDivElement | undefined;
  return new UnityVoiceBridge({
    origin: globalThis.location?.origin ?? '',
    sendMessage: (gameObjectName, methodName, payload) =>
      globalThis.window?.Module?.SendMessage?.(gameObjectName, methodName, payload),
    createController: (callbacks) => {
      audioSink ??= createAudioSink();
      return new VoiceMediaController({
        tokenProvider: new HttpVoiceTokenProvider({ apiBaseUrl: '/sandflow/api/v1' }),
        roomFactory: new LiveKitBrowserRoomFactory(audioSink),
        callbacks,
      });
    },
  });
}

function createAudioSink(): HTMLDivElement {
  const sink = document.createElement('div');
  sink.id = 'sandflow-voice-audio';
  sink.hidden = true;
  sink.setAttribute('aria-hidden', 'true');
  document.body.appendChild(sink);
  return sink;
}

const productionBridge = createProductionBridge();

export const initialize = (gameObjectName: string, enabled: number): number =>
  productionBridge.initialize(gameObjectName, enabled === 1) ? 1 : 0;
export const connect = (sandboxId: string): void => productionBridge.connect(sandboxId);
export const enableMicrophone = (): void => productionBridge.enableMicrophone();
export const disableMicrophone = (): void => productionBridge.disableMicrophone();
export const setPushToTalkPressed = (pressed: number): void => productionBridge.setPushToTalkPressed(pressed === 1);
export const setSelfMuted = (muted: number): void => productionBridge.setSelfMuted(muted === 1);
export const setRemoteMuted = (participantId: string, muted: number): void =>
  productionBridge.setRemoteMuted(participantId, muted === 1);
export const setRemoteVolume = (participantId: string, volume: number): void =>
  productionBridge.setRemoteVolume(participantId, volume);
export const setRemoteBlocked = (participantId: string, blocked: number): void =>
  productionBridge.setRemoteBlocked(participantId, blocked === 1);
export const startAudio = (): void => productionBridge.startAudioFromUserGesture();
export const disconnect = (): void => productionBridge.disconnect();
