namespace SandFlow.Server;

/// <summary>Burst headroom for two-command tools sampled at 30 Hz, without removing abuse limits.</summary>
public sealed class InputCommandBudget(DateTimeOffset now)
{
    public const double Capacity=120;
    private double tokens=Capacity;
    private DateTimeOffset updated=now;
    public bool Take(DateTimeOffset time)
    {
        tokens=Math.Min(Capacity,tokens+Math.Max(0,(time-updated).TotalSeconds)*Capacity);
        if(time>updated)updated=time;
        if(tokens<1)return false;
        tokens-=1;return true;
    }
}
