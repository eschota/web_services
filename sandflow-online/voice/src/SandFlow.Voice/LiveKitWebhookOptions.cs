namespace SandFlow.Voice;

public sealed record LiveKitWebhookOptions
{
    public bool Enabled { get; init; }
    public string ApiKey { get; init; } = string.Empty;
    public string ApiSecret { get; init; } = string.Empty;
    public TimeSpan MaximumTokenAge { get; init; } = TimeSpan.FromMinutes(5);
    public TimeSpan ClockSkew { get; init; } = TimeSpan.FromSeconds(30);
    public int ReplayCacheCapacity { get; init; } = 4096;
    public int MaximumBodyBytes { get; init; } = 256 * 1024;
}
