namespace SandFlow.Voice;

/// <summary>A server-trusted participant location. Never construct this from client room or team claims.</summary>
public sealed record VoiceControlTarget(
    string RoomId,
    string ParticipantId,
    VoiceMode Mode,
    int? TeamId);
