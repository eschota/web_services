namespace SandFlow.Voice;

public sealed record VoiceTokenOptions
{
    public bool Enabled { get; init; }
    public string ServerUrl { get; init; } = string.Empty;
    public string ApiKey { get; init; } = string.Empty;
    public string ApiSecret { get; init; } = string.Empty;
    public TimeSpan TokenLifetime { get; init; } = TimeSpan.FromMinutes(5);

    public static VoiceTokenOptions FromEnvironment()
    {
        var enabled = bool.TryParse(Environment.GetEnvironmentVariable("SANDFLOW_VOICE_ENABLED"), out var value) && value;
        return new VoiceTokenOptions
        {
            Enabled = enabled,
            ServerUrl = Environment.GetEnvironmentVariable("SANDFLOW_VOICE_SERVER_URL") ?? string.Empty,
            ApiKey = Environment.GetEnvironmentVariable("SANDFLOW_VOICE_API_KEY") ?? string.Empty,
            ApiSecret = Environment.GetEnvironmentVariable("SANDFLOW_VOICE_API_SECRET") ?? string.Empty,
        };
    }
}
