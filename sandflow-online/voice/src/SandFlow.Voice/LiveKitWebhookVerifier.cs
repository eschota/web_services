using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace SandFlow.Voice;

/// <summary>Verifies LiveKit's HS256 authorization JWT and its SHA-256 binding to the raw webhook body.</summary>
public sealed partial class LiveKitWebhookVerifier
{
    private readonly LiveKitWebhookOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly object _replayLock = new();
    private readonly Dictionary<string, DateTimeOffset> _replayExpirations = new(StringComparer.Ordinal);
    private readonly Queue<string> _replayOrder = new();

    public LiveKitWebhookVerifier(LiveKitWebhookOptions options, TimeProvider? timeProvider = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public VerifiedLiveKitWebhook Verify(ReadOnlySpan<byte> rawBody, string authorizationHeader)
    {
        ValidateOptions();
        if (rawBody.IsEmpty || rawBody.Length > _options.MaximumBodyBytes)
            throw new LiveKitWebhookException("Webhook body is empty or exceeds the configured limit.");
        if (string.IsNullOrWhiteSpace(authorizationHeader))
            throw new LiveKitWebhookException("Webhook authorization header is missing.");

        var token = authorizationHeader.StartsWith("Bearer ", StringComparison.Ordinal)
            ? authorizationHeader[7..]
            : authorizationHeader;
        var pieces = token.Split('.');
        if (pieces.Length != 3 || pieces.Any(string.IsNullOrEmpty))
            throw new LiveKitWebhookException("Webhook authorization token is malformed.");

        using var header = ParseBase64UrlJson(pieces[0], "header");
        if (!header.RootElement.TryGetProperty("alg", out var algorithm) ||
            algorithm.ValueKind != JsonValueKind.String || algorithm.GetString() != "HS256")
            throw new LiveKitWebhookException("Webhook token must use HS256.");

        var unsigned = $"{pieces[0]}.{pieces[1]}";
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(_options.ApiSecret));
        var expectedSignature = hmac.ComputeHash(Encoding.ASCII.GetBytes(unsigned));
        var actualSignature = DecodeBase64Url(pieces[2], "signature");
        if (!CryptographicOperations.FixedTimeEquals(expectedSignature, actualSignature))
            throw new LiveKitWebhookException("Webhook token signature is invalid.");

        using var claimsDocument = ParseBase64UrlJson(pieces[1], "claims");
        var claims = claimsDocument.RootElement;
        if (!claims.TryGetProperty("iss", out var issuer) ||
            issuer.ValueKind != JsonValueKind.String || issuer.GetString() != _options.ApiKey)
            throw new LiveKitWebhookException("Webhook token issuer is invalid.");

        var now = _timeProvider.GetUtcNow();
        var issuedAt = ReadUnixTime(claims, "iat");
        var expiresAt = ReadUnixTime(claims, "exp");
        var notBefore = claims.TryGetProperty("nbf", out _) ? ReadUnixTime(claims, "nbf") : issuedAt;
        if (notBefore > now.Add(_options.ClockSkew))
            throw new LiveKitWebhookException("Webhook token is not active yet.");
        if (expiresAt < now.Subtract(_options.ClockSkew))
            throw new LiveKitWebhookException("Webhook token has expired.");
        if (issuedAt > now.Add(_options.ClockSkew) || issuedAt < now.Subtract(_options.MaximumTokenAge).Subtract(_options.ClockSkew))
            throw new LiveKitWebhookException("Webhook token issue time is outside the accepted window.");
        if (expiresAt <= issuedAt || expiresAt - issuedAt > _options.MaximumTokenAge.Add(_options.ClockSkew))
            throw new LiveKitWebhookException("Webhook token lifetime is invalid.");

        var claimedHash = claims.TryGetProperty("sha256", out var hashClaim) && hashClaim.ValueKind == JsonValueKind.String
            ? DecodeStandardBase64(hashClaim.GetString(), "sha256")
            : throw new LiveKitWebhookException("Webhook token is missing sha256.");
        var actualHash = SHA256.HashData(rawBody);
        if (!CryptographicOperations.FixedTimeEquals(claimedHash, actualHash))
            throw new LiveKitWebhookException("Webhook body hash does not match the signed claim.");

        JsonElement payload;
        string id;
        string eventName;
        DateTimeOffset createdAt;
        try
        {
            using var bodyDocument = JsonDocument.Parse(rawBody.ToArray());
            var root = bodyDocument.RootElement;
            id = root.GetProperty("id").GetString() ?? string.Empty;
            eventName = root.GetProperty("event").GetString() ?? string.Empty;
            createdAt = DateTimeOffset.FromUnixTimeSeconds(root.GetProperty("createdAt").GetInt64());
            if (!EventIdRegex().IsMatch(id) || !EventNameRegex().IsMatch(eventName))
                throw new LiveKitWebhookException("Webhook event identity is invalid.");
            payload = root.Clone();
        }
        catch (LiveKitWebhookException)
        {
            throw;
        }
        catch (Exception error) when (error is JsonException or InvalidOperationException or ArgumentOutOfRangeException or KeyNotFoundException)
        {
            throw new LiveKitWebhookException("Webhook body is not a valid LiveKit event envelope.");
        }

        var replayKey = claims.TryGetProperty("jti", out var jti) &&
            jti.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(jti.GetString())
            ? $"jti:{jti.GetString()}"
            : $"token:{Convert.ToHexString(SHA256.HashData(Encoding.ASCII.GetBytes(token)))}";
        RegisterReplayKey(replayKey, expiresAt.Add(_options.ClockSkew), now);

        return new VerifiedLiveKitWebhook(id, eventName, createdAt, payload);
    }

    private void ValidateOptions()
    {
        if (!_options.Enabled)
            throw new LiveKitWebhookException("LiveKit webhook verification is disabled.");
        if (!ApiKeyRegex().IsMatch(_options.ApiKey))
            throw new LiveKitWebhookException("Webhook ApiKey is missing or invalid.");
        if (string.IsNullOrWhiteSpace(_options.ApiSecret) || _options.ApiSecret.Length < 32)
            throw new LiveKitWebhookException("Webhook ApiSecret must contain at least 32 characters.");
        if (_options.MaximumTokenAge <= TimeSpan.Zero || _options.MaximumTokenAge > TimeSpan.FromMinutes(10))
            throw new LiveKitWebhookException("MaximumTokenAge is invalid.");
        if (_options.ClockSkew < TimeSpan.Zero || _options.ClockSkew > TimeSpan.FromMinutes(1))
            throw new LiveKitWebhookException("ClockSkew is invalid.");
        if (_options.ReplayCacheCapacity is < 16 or > 100_000)
            throw new LiveKitWebhookException("ReplayCacheCapacity is invalid.");
        if (_options.MaximumBodyBytes is < 1024 or > 4 * 1024 * 1024)
            throw new LiveKitWebhookException("MaximumBodyBytes is invalid.");
    }

    private void RegisterReplayKey(string key, DateTimeOffset expiration, DateTimeOffset now)
    {
        lock (_replayLock)
        {
            while (_replayOrder.Count > 0)
            {
                var oldest = _replayOrder.Peek();
                if (_replayExpirations.TryGetValue(oldest, out var oldestExpiration) &&
                    oldestExpiration > now)
                    break;
                _replayOrder.Dequeue();
                _replayExpirations.Remove(oldest);
            }

            if (_replayExpirations.ContainsKey(key))
                throw new LiveKitWebhookException("Webhook replay was rejected.");

            if (_replayExpirations.Count >= _options.ReplayCacheCapacity)
                throw new LiveKitWebhookException("Webhook replay cache is full; verification failed closed.");

            _replayExpirations.Add(key, expiration);
            _replayOrder.Enqueue(key);
        }
    }

    private static DateTimeOffset ReadUnixTime(JsonElement claims, string name)
    {
        if (!claims.TryGetProperty(name, out var value) || !value.TryGetInt64(out var seconds))
            throw new LiveKitWebhookException($"Webhook token is missing {name}.");
        try { return DateTimeOffset.FromUnixTimeSeconds(seconds); }
        catch (ArgumentOutOfRangeException) { throw new LiveKitWebhookException($"Webhook token {name} is invalid."); }
    }

    private static JsonDocument ParseBase64UrlJson(string encoded, string field)
    {
        try { return JsonDocument.Parse(DecodeBase64Url(encoded, field)); }
        catch (JsonException) { throw new LiveKitWebhookException($"Webhook token {field} is invalid JSON."); }
    }

    private static byte[] DecodeBase64Url(string encoded, string field)
    {
        try
        {
            var value = encoded.Replace('-', '+').Replace('_', '/');
            value = value.PadRight(value.Length + ((4 - value.Length % 4) % 4), '=');
            return Convert.FromBase64String(value);
        }
        catch (FormatException) { throw new LiveKitWebhookException($"Webhook token {field} is invalid base64url."); }
    }

    private static byte[] DecodeStandardBase64(string? encoded, string field)
    {
        if (string.IsNullOrWhiteSpace(encoded))
            throw new LiveKitWebhookException($"Webhook token is missing {field}.");
        try { return Convert.FromBase64String(encoded); }
        catch (FormatException) { throw new LiveKitWebhookException($"Webhook token {field} is invalid base64."); }
    }

    [GeneratedRegex("^[A-Za-z0-9][A-Za-z0-9_-]{2,63}$", RegexOptions.CultureInvariant)]
    private static partial Regex ApiKeyRegex();

    [GeneratedRegex("^[A-Za-z0-9][A-Za-z0-9_-]{7,127}$", RegexOptions.CultureInvariant)]
    private static partial Regex EventIdRegex();

    [GeneratedRegex("^[a-z][a-z0-9_]{2,63}$", RegexOptions.CultureInvariant)]
    private static partial Regex EventNameRegex();
}
