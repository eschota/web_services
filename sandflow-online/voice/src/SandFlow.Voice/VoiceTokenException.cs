namespace SandFlow.Voice;

public sealed class VoiceTokenException : InvalidOperationException
{
    public VoiceTokenException(string message) : base(message) { }
}
