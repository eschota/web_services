namespace SandFlow.Voice;

public sealed record LiveKitAdminOptions
{
    public bool Enabled { get; init; }
    public string ServerHttpUrl { get; init; } = string.Empty;
    public string ApiKey { get; init; } = string.Empty;
    public string ApiSecret { get; init; } = string.Empty;
    public TimeSpan RequestTokenLifetime { get; init; } = TimeSpan.FromMinutes(1);
}
