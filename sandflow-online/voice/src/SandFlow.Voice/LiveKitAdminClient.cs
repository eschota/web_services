using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace SandFlow.Voice;

/// <summary>Minimal LiveKit RoomService Twirp client for server-side voice enforcement.</summary>
public sealed partial class LiveKitAdminClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly LiveKitAdminOptions _options;
    private readonly HttpClient _httpClient;
    private readonly TimeProvider _timeProvider;

    public LiveKitAdminClient(
        LiveKitAdminOptions options,
        HttpClient httpClient,
        TimeProvider? timeProvider = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task SetMicrophonePermissionAsync(
        VoiceControlTarget target,
        bool canPublish,
        CancellationToken cancellationToken = default)
    {
        var room = ValidateAndResolveRoom(target);
        var body = new
        {
            room,
            identity = target.ParticipantId,
            permission = new
            {
                canSubscribe = true,
                canPublish,
                canPublishData = false,
                canPublishSources = new[] { "MICROPHONE" },
            },
        };
        return SendAsync("UpdateParticipant", room, body, cancellationToken);
    }

    public Task MutePublishedTrackAsync(
        VoiceControlTarget target,
        string trackSid,
        bool muted = true,
        CancellationToken cancellationToken = default)
    {
        var room = ValidateAndResolveRoom(target);
        if (string.IsNullOrWhiteSpace(trackSid) || !TrackSidRegex().IsMatch(trackSid))
            throw new LiveKitAdminException("LiveKit microphone TrackSid is invalid.");

        return SendAsync(
            "MutePublishedTrack",
            room,
            new { room, identity = target.ParticipantId, trackSid, muted },
            cancellationToken);
    }

    public Task RemoveParticipantAsync(
        VoiceControlTarget target,
        CancellationToken cancellationToken = default)
    {
        var room = ValidateAndResolveRoom(target);
        return SendAsync(
            "RemoveParticipant",
            room,
            new { room, identity = target.ParticipantId },
            cancellationToken);
    }

    private async Task SendAsync(string method, string room, object body, CancellationToken cancellationToken)
    {
        ValidateOptions();
        var endpoint = new Uri(
            EnsureTrailingSlash(new Uri(_options.ServerHttpUrl, UriKind.Absolute)),
            $"twirp/livekit.RoomService/{method}");
        using var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
        {
            Content = new StringContent(JsonSerializer.Serialize(body, JsonOptions), Encoding.UTF8, "application/json"),
        };
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", CreateAdminToken(room));

        HttpResponseMessage response;
        try
        {
            response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception error) when (error is HttpRequestException or TaskCanceledException)
        {
            throw new LiveKitAdminException($"LiveKit {method} request failed before a response was received.", error);
        }

        using (response)
        {
            if (response.IsSuccessStatusCode)
                return;

            var detail = await ReadBoundedErrorAsync(response, cancellationToken).ConfigureAwait(false);
            throw new LiveKitAdminException(
                $"LiveKit {method} failed with HTTP {(int)response.StatusCode} ({response.StatusCode}){detail}.");
        }
    }

    private string ValidateAndResolveRoom(VoiceControlTarget target)
    {
        ArgumentNullException.ThrowIfNull(target);
        if (target.Mode is not (VoiceMode.Coop or VoiceMode.Pvp))
            throw new LiveKitAdminException("Voice mode is invalid.");
        if (!IdentifierRegex().IsMatch(target.RoomId))
            throw new LiveKitAdminException("RoomId is invalid.");
        if (!ParticipantRegex().IsMatch(target.ParticipantId))
            throw new LiveKitAdminException("ParticipantId is invalid.");
        if (target.Mode == VoiceMode.Pvp && target.TeamId is not (0 or 1))
            throw new LiveKitAdminException("PvP target requires server-assigned TeamId 0 or 1.");
        if (target.Mode == VoiceMode.Coop && target.TeamId is not null)
            throw new LiveKitAdminException("Co-op target must not contain a TeamId.");

        return target.Mode == VoiceMode.Coop
            ? $"sf:{target.RoomId}:all"
            : $"sf:{target.RoomId}:team:{target.TeamId}";
    }

    private void ValidateOptions()
    {
        if (!_options.Enabled)
            throw new LiveKitAdminException("LiveKit administration is disabled.");
        if (!Uri.TryCreate(_options.ServerHttpUrl, UriKind.Absolute, out var uri) || !IsAllowedServerUri(uri))
            throw new LiveKitAdminException("ServerHttpUrl must use https:// or loopback http://.");
        if (!IdentifierRegex().IsMatch(_options.ApiKey))
            throw new LiveKitAdminException("LiveKit ApiKey is missing or invalid.");
        if (string.IsNullOrWhiteSpace(_options.ApiSecret) || _options.ApiSecret.Length < 32)
            throw new LiveKitAdminException("LiveKit ApiSecret must contain at least 32 characters.");
        if (_options.RequestTokenLifetime <= TimeSpan.Zero || _options.RequestTokenLifetime > TimeSpan.FromMinutes(1))
            throw new LiveKitAdminException("Admin request token lifetime must be between zero and one minute.");
    }

    private static bool IsAllowedServerUri(Uri uri) =>
        uri.Scheme == Uri.UriSchemeHttps ||
        (uri.Scheme == Uri.UriSchemeHttp && (uri.IsLoopback || uri.Host is "127.0.0.1" or "::1"));

    private string CreateAdminToken(string room)
    {
        var now = _timeProvider.GetUtcNow();
        var header = EncodeJson(new Dictionary<string, object> { ["alg"] = "HS256", ["typ"] = "JWT" });
        var payload = EncodeJson(new Dictionary<string, object>
        {
            ["iss"] = _options.ApiKey,
            ["iat"] = now.ToUnixTimeSeconds(),
            ["nbf"] = now.ToUnixTimeSeconds(),
            ["exp"] = now.Add(_options.RequestTokenLifetime).ToUnixTimeSeconds(),
            ["jti"] = Guid.NewGuid().ToString("N"),
            ["video"] = new Dictionary<string, object> { ["roomAdmin"] = true, ["room"] = room },
        });
        var unsigned = $"{header}.{payload}";
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(_options.ApiSecret));
        return $"{unsigned}.{Base64UrlEncode(hmac.ComputeHash(Encoding.ASCII.GetBytes(unsigned)))}";
    }

    private static string EncodeJson(object value) => Base64UrlEncode(JsonSerializer.SerializeToUtf8Bytes(value, JsonOptions));
    private static string Base64UrlEncode(ReadOnlySpan<byte> value) =>
        Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    private static Uri EnsureTrailingSlash(Uri uri) => uri.AbsoluteUri.EndsWith('/') ? uri : new Uri(uri.AbsoluteUri + "/");

    private static async Task<string> ReadBoundedErrorAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.Content is null)
            return string.Empty;
        var text = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (text.Length > 512)
            text = text[..512];
        text = text.Replace('\r', ' ').Replace('\n', ' ').Trim();
        return text.Length == 0 ? string.Empty : $": {text}";
    }

    [GeneratedRegex("^[A-Za-z0-9][A-Za-z0-9_-]{2,63}$", RegexOptions.CultureInvariant)]
    private static partial Regex IdentifierRegex();

    [GeneratedRegex("^[A-Za-z0-9][A-Za-z0-9_.:@-]{2,127}$", RegexOptions.CultureInvariant)]
    private static partial Regex ParticipantRegex();

    [GeneratedRegex("^TR_[A-Za-z0-9]{6,64}$", RegexOptions.CultureInvariant)]
    private static partial Regex TrackSidRegex();
}
