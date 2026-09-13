namespace SandFlow.Voice;

public sealed class LiveKitWebhookException : InvalidOperationException
{
    public LiveKitWebhookException(string message) : base(message) { }
}
