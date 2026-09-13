namespace SandFlow.Voice;

public sealed record VoiceTokenResult(
    string ServerUrl,
    string Token,
    string RoomName,
    string ParticipantId,
    DateTimeOffset ExpiresAt);
