using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace SandFlow.Voice;

public sealed partial class VoiceTokenService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly VoiceTokenOptions _options;
    private readonly TimeProvider _timeProvider;

    public VoiceTokenService(VoiceTokenOptions options, TimeProvider? timeProvider = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public VoiceTokenResult IssueToken(VoiceMembership membership)
    {
        ArgumentNullException.ThrowIfNull(membership);
        ValidateOptions();
        ValidateMembership(membership);

        var now = _timeProvider.GetUtcNow();
        var expiresAt = now.Add(_options.TokenLifetime);
        var roomName = membership.Mode == VoiceMode.Coop
            ? $"sf:{membership.RoomId}:all"
            : $"sf:{membership.RoomId}:team:{membership.TeamId}";

        var header = new Dictionary<string, object>
        {
            ["alg"] = "HS256",
            ["typ"] = "JWT",
        };
        var videoGrant = new Dictionary<string, object>
        {
            ["roomJoin"] = true,
            ["room"] = roomName,
            ["canSubscribe"] = true,
            ["canPublish"] = membership.CanSpeak,
            ["canPublishData"] = false,
            ["canPublishSources"] = new[] { "microphone" },
        };
        var payload = new Dictionary<string, object>
        {
            ["iss"] = _options.ApiKey,
            ["sub"] = membership.ParticipantId,
            ["iat"] = now.ToUnixTimeSeconds(),
            ["nbf"] = now.ToUnixTimeSeconds(),
            ["exp"] = expiresAt.ToUnixTimeSeconds(),
            ["jti"] = Guid.NewGuid().ToString("N"),
            ["video"] = videoGrant,
        };

        var unsigned = $"{EncodeJson(header)}.{EncodeJson(payload)}";
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(_options.ApiSecret));
        var signature = Base64UrlEncode(hmac.ComputeHash(Encoding.ASCII.GetBytes(unsigned)));

        return new VoiceTokenResult(
            _options.ServerUrl,
            $"{unsigned}.{signature}",
            roomName,
            membership.ParticipantId,
            expiresAt);
    }

    private void ValidateOptions()
    {
        if (!_options.Enabled)
            throw new VoiceTokenException("SandFlow voice is disabled.");
        if (!Uri.TryCreate(_options.ServerUrl, UriKind.Absolute, out var uri) || uri.Scheme != "wss")
            throw new VoiceTokenException("Voice ServerUrl must be an absolute wss:// URL.");
        if (!IdentifierRegex().IsMatch(_options.ApiKey))
            throw new VoiceTokenException("Voice ApiKey is missing or invalid.");
        if (string.IsNullOrWhiteSpace(_options.ApiSecret) || _options.ApiSecret.Length < 32)
            throw new VoiceTokenException("Voice ApiSecret must contain at least 32 characters.");
        if (_options.TokenLifetime <= TimeSpan.Zero || _options.TokenLifetime > TimeSpan.FromMinutes(5))
            throw new VoiceTokenException("Voice token lifetime must be between zero and five minutes.");
    }

    private static void ValidateMembership(VoiceMembership membership)
    {
        if (!membership.IsAdmitted)
            throw new VoiceTokenException("Participant is not admitted to this sandbox.");
        if (membership.Mode is not (VoiceMode.Coop or VoiceMode.Pvp))
            throw new VoiceTokenException("Voice mode is invalid.");
        if (!IdentifierRegex().IsMatch(membership.RoomId))
            throw new VoiceTokenException("RoomId is invalid.");
        if (!ParticipantRegex().IsMatch(membership.ParticipantId))
            throw new VoiceTokenException("ParticipantId is invalid.");
        if (membership.Mode == VoiceMode.Pvp && membership.TeamId is not (0 or 1))
            throw new VoiceTokenException("PvP membership requires server-assigned TeamId 0 or 1.");
        if (membership.Mode == VoiceMode.Coop && membership.TeamId is not null)
            throw new VoiceTokenException("Co-op membership must not contain a TeamId.");
    }

    private static string EncodeJson(object value) =>
        Base64UrlEncode(JsonSerializer.SerializeToUtf8Bytes(value, JsonOptions));

    private static string Base64UrlEncode(ReadOnlySpan<byte> value) =>
        Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    [GeneratedRegex("^[A-Za-z0-9][A-Za-z0-9_-]{2,63}$", RegexOptions.CultureInvariant)]
    private static partial Regex IdentifierRegex();

    [GeneratedRegex("^[A-Za-z0-9][A-Za-z0-9_.:@-]{2,127}$", RegexOptions.CultureInvariant)]
    private static partial Regex ParticipantRegex();
}
