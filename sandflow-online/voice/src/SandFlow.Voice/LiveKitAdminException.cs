namespace SandFlow.Voice;

public sealed class LiveKitAdminException : InvalidOperationException
{
    public LiveKitAdminException(string message) : base(message) { }
    public LiveKitAdminException(string message, Exception innerException) : base(message, innerException) { }
}
