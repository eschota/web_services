namespace SandFlow.Voice;

/// <summary>Trusted membership produced by the SandFlow backend after admission and team checks.</summary>
public sealed record VoiceMembership(
    string RoomId,
    string ParticipantId,
    VoiceMode Mode,
    int? TeamId,
    bool CanSpeak,
    bool IsAdmitted);
