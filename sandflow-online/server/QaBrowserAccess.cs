using System.Security.Cryptography;
using System.Text;

namespace SandFlow.Server;

public sealed record QaBrowserChallenge(string Code, string CookieValue, DateTimeOffset Expires, bool Approved);
public sealed record QaBrowserClaim(SessionGrant Session, string WorldId);

/// <summary>One browser proves possession via an HttpOnly nonce; existing QA authority approves only its public code.</summary>
public sealed class QaBrowserAccess(TimeProvider time)
{
    private sealed class Entry(string code, byte[] secretHash, DateTimeOffset expires)
    {
        public string Code { get; } = code;
        public byte[] SecretHash { get; } = secretHash;
        public DateTimeOffset Expires { get; } = expires;
        public string? WorldId { get; set; }
        public SessionGrant? Session { get; set; }
    }
    private readonly Dictionary<string, Entry> _entries = [];
    private readonly object _gate = new();
    public QaBrowserChallenge Begin(string? cookie)
    {
        lock (_gate)
        {
            Sweep();
            var existing = Match(cookie);
            if (existing != null) return new(existing.Code, cookie!, existing.Expires, existing.WorldId != null);
            if (_entries.Count >= 128) throw new ApiFailure(429, "qa_challenge_capacity");
            var code = Protocol.RandomId(); var secret = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
            var entry = new Entry(code, SHA256.HashData(Encoding.ASCII.GetBytes(secret)), time.GetUtcNow().AddMinutes(5));
            _entries.Add(code, entry);
            return new(code, code + "." + secret, entry.Expires, false);
        }
    }
    public void Approve(string? code, string worldId, bool authorized)
    {
        if (!authorized) throw new ApiFailure(403, "qa_authority_required");
        if (!Protocol.ValidId(worldId)) throw new ApiFailure(400, "qa_world");
        lock (_gate)
        {
            Sweep();
            if (code == null || !_entries.TryGetValue(code, out var entry)) throw new ApiFailure(404, "qa_challenge_expired");
            if (entry.WorldId != null && entry.WorldId != worldId) throw new ApiFailure(409, "qa_challenge_already_bound");
            entry.WorldId = worldId;
        }
    }
    public QaBrowserClaim Claim(string? cookie, Func<SessionGrant> create)
    {
        lock (_gate)
        {
            Sweep();
            var entry = Match(cookie) ?? throw new ApiFailure(403, "qa_browser_proof_required");
            if (entry.WorldId == null) throw new ApiFailure(409, "qa_approval_pending");
            entry.Session ??= create(); // Same-browser retry is idempotent; never create another guest on a lost response.
            return new(entry.Session, entry.WorldId);
        }
    }
    private Entry? Match(string? cookie)
    {
        if (cookie == null || cookie.Length != 97 || cookie[32] != '.' || !_entries.TryGetValue(cookie[..32], out var entry)) return null;
        var secret = cookie[33..];
        if (!secret.All(Uri.IsHexDigit)) return null;
        return CryptographicOperations.FixedTimeEquals(entry.SecretHash, SHA256.HashData(Encoding.ASCII.GetBytes(secret))) ? entry : null;
    }
    private void Sweep()
    { foreach (var code in _entries.Where(x => x.Value.Expires <= time.GetUtcNow()).Select(x => x.Key).ToArray()) _entries.Remove(code); }
}
