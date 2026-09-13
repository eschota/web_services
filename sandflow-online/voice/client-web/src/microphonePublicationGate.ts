export interface StoppableMicrophoneTrack {
  stop(): void;
}

/** Separates device acquisition from publication so revoked pending capture is never published. */
export class MicrophonePublicationGate<TTrack extends StoppableMicrophoneTrack> {
  #generation = 0;
  #pending: Promise<boolean> | undefined;

  invalidate(): void {
    this.#generation++;
  }

  async captureThenPublish(
    isPublished: () => boolean,
    capture: () => Promise<TTrack>,
    publish: (track: TTrack) => Promise<void>,
    stopPublished: () => Promise<void>,
  ): Promise<boolean> {
    if (isPublished()) return true;
    if (this.#pending) return this.#pending;

    const operation = this.#captureThenPublish(capture, publish, stopPublished);
    this.#pending = operation;
    try {
      return await operation;
    } finally {
      if (this.#pending === operation) this.#pending = undefined;
    }
  }

  async #captureThenPublish(
    capture: () => Promise<TTrack>,
    publish: (track: TTrack) => Promise<void>,
    stopPublished: () => Promise<void>,
  ): Promise<boolean> {
    const generation = ++this.#generation;
    const track = await capture();
    if (generation !== this.#generation) {
      track.stop();
      return false;
    }

    try {
      await publish(track);
    } catch (error) {
      track.stop();
      throw error;
    }

    if (generation !== this.#generation) {
      await stopPublished().catch(() => undefined);
      return false;
    }
    return true;
  }
}
