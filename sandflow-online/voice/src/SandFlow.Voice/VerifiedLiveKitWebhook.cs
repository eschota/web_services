using System.Text.Json;

namespace SandFlow.Voice;

public sealed record VerifiedLiveKitWebhook(
    string Id,
    string Event,
    DateTimeOffset CreatedAt,
    JsonElement Payload);
