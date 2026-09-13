using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Net;
using SandFlow.Voice;

var tests = new (string Name, Action Run)[]
{
    ("team isolation", TeamIsolation),
    ("unauthorized admission", UnauthorizedAdmission),
    ("microphone-only grants", MicrophoneOnlyGrants),
    ("expiry and signature", ExpiryAndSignature),
    ("invalid configuration", InvalidConfiguration),
    ("invalid membership", InvalidMembership),
    ("admin Twirp contracts", AdminTwirpContracts),
    ("admin rejects untrusted targets", AdminRejectsUntrustedTargets),
    ("webhook verification", WebhookVerification),
    ("webhook tamper expiry and replay", WebhookTamperExpiryAndReplay),
};

var failed = 0;
foreach (var test in tests)
{
    try
    {
        test.Run();
        Console.WriteLine($"PASS {test.Name}");
    }
    catch (Exception error)
    {
        failed++;
        Console.Error.WriteLine($"FAIL {test.Name}: {error.Message}");
    }
}

return failed == 0 ? 0 : 1;

static VoiceTokenService CreateService(TimeProvider? clock = null, VoiceTokenOptions? options = null) =>
    new(options ?? ValidOptions(), clock);

static VoiceTokenOptions ValidOptions() => new()
{
    Enabled = true,
    ServerUrl = "wss://autorig.online/sandflow/voice",
    ApiKey = "sandflow-key",
    ApiSecret = "0123456789abcdef0123456789abcdef",
};

static VoiceMembership Member(VoiceMode mode, int? team = null, bool canSpeak = true, bool admitted = true) =>
    new("beach_001", "steam:76561198000000000", mode, team, canSpeak, admitted);

static void TeamIsolation()
{
    var service = CreateService();
    Equal("sf:beach_001:team:0", service.IssueToken(Member(VoiceMode.Pvp, 0)).RoomName);
    Equal("sf:beach_001:team:1", service.IssueToken(Member(VoiceMode.Pvp, 1)).RoomName);
    Equal("sf:beach_001:all", service.IssueToken(Member(VoiceMode.Coop)).RoomName);
}

static void UnauthorizedAdmission() =>
    Throws<VoiceTokenException>(() => CreateService().IssueToken(Member(VoiceMode.Coop, admitted: false)));

static void MicrophoneOnlyGrants()
{
    var token = CreateService().IssueToken(Member(VoiceMode.Pvp, 0)).Token;
    using var payload = ReadPayload(token);
    var video = payload.RootElement.GetProperty("video");
    True(video.GetProperty("roomJoin").GetBoolean());
    True(video.GetProperty("canPublish").GetBoolean());
    True(video.GetProperty("canSubscribe").GetBoolean());
    False(video.GetProperty("canPublishData").GetBoolean());
    Equal("microphone", video.GetProperty("canPublishSources")[0].GetString() ?? string.Empty);
    Equal(1, video.GetProperty("canPublishSources").GetArrayLength());

    using var listener = ReadPayload(CreateService().IssueToken(Member(VoiceMode.Coop, canSpeak: false)).Token);
    False(listener.RootElement.GetProperty("video").GetProperty("canPublish").GetBoolean());
}

static void ExpiryAndSignature()
{
    var instant = new DateTimeOffset(2026, 9, 13, 10, 0, 0, TimeSpan.Zero);
    var service = CreateService(new FixedTimeProvider(instant));
    var result = service.IssueToken(Member(VoiceMode.Coop));
    Equal(instant.AddMinutes(5), result.ExpiresAt);
    using var payload = ReadPayload(result.Token);
    Equal(instant.ToUnixTimeSeconds(), payload.RootElement.GetProperty("iat").GetInt64());
    Equal(instant.AddMinutes(5).ToUnixTimeSeconds(), payload.RootElement.GetProperty("exp").GetInt64());

    var pieces = result.Token.Split('.');
    using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(ValidOptions().ApiSecret));
    var expected = Base64Url(hmac.ComputeHash(Encoding.ASCII.GetBytes($"{pieces[0]}.{pieces[1]}")));
    Equal(expected, pieces[2]);
}

static void InvalidConfiguration()
{
    Throws<VoiceTokenException>(() => CreateService(options: new VoiceTokenOptions()).IssueToken(Member(VoiceMode.Coop)));
    Throws<VoiceTokenException>(() => CreateService(options: ValidOptions() with { ServerUrl = "ws://localhost:7880" }).IssueToken(Member(VoiceMode.Coop)));
    Throws<VoiceTokenException>(() => CreateService(options: ValidOptions() with { ApiSecret = "short" }).IssueToken(Member(VoiceMode.Coop)));
    Throws<VoiceTokenException>(() => CreateService(options: ValidOptions() with { TokenLifetime = TimeSpan.FromMinutes(6) }).IssueToken(Member(VoiceMode.Coop)));
}

static void InvalidMembership()
{
    Throws<VoiceTokenException>(() => CreateService().IssueToken(Member(VoiceMode.Pvp, null)));
    Throws<VoiceTokenException>(() => CreateService().IssueToken(Member(VoiceMode.Pvp, 2)));
    Throws<VoiceTokenException>(() => CreateService().IssueToken(Member(VoiceMode.Coop, 0)));
    Throws<VoiceTokenException>(() => CreateService().IssueToken(Member(VoiceMode.Coop) with { RoomId = "bad:room" }));
}

static void AdminTwirpContracts()
{
    var clock = new FixedTimeProvider(new DateTimeOffset(2026, 9, 13, 11, 0, 0, TimeSpan.Zero));
    var handler = new CaptureHandler();
    using var http = new HttpClient(handler);
    var admin = new LiveKitAdminClient(ValidAdminOptions(), http, clock);
    var target = new VoiceControlTarget("beach_001", "steam:76561198000000000", VoiceMode.Pvp, 1);

    admin.SetMicrophonePermissionAsync(target, false).GetAwaiter().GetResult();
    admin.MutePublishedTrackAsync(target, "TR_A1b2C3d4", true).GetAwaiter().GetResult();
    admin.RemoveParticipantAsync(target).GetAwaiter().GetResult();

    Equal(3, handler.Requests.Count);
    Equal("/twirp/livekit.RoomService/UpdateParticipant", handler.Requests[0].Path);
    Equal("/twirp/livekit.RoomService/MutePublishedTrack", handler.Requests[1].Path);
    Equal("/twirp/livekit.RoomService/RemoveParticipant", handler.Requests[2].Path);

    using var update = JsonDocument.Parse(handler.Requests[0].Body);
    Equal("sf:beach_001:team:1", update.RootElement.GetProperty("room").GetString() ?? string.Empty);
    var permission = update.RootElement.GetProperty("permission");
    False(permission.GetProperty("canPublish").GetBoolean());
    True(permission.GetProperty("canSubscribe").GetBoolean());
    False(permission.GetProperty("canPublishData").GetBoolean());
    Equal("MICROPHONE", permission.GetProperty("canPublishSources")[0].GetString() ?? string.Empty);

    using var adminClaims = ReadPayload(handler.Requests[0].BearerToken);
    True(adminClaims.RootElement.GetProperty("video").GetProperty("roomAdmin").GetBoolean());
    Equal("sf:beach_001:team:1", adminClaims.RootElement.GetProperty("video").GetProperty("room").GetString() ?? string.Empty);
    Equal(clock.GetUtcNow().AddMinutes(1).ToUnixTimeSeconds(), adminClaims.RootElement.GetProperty("exp").GetInt64());

    using var mute = JsonDocument.Parse(handler.Requests[1].Body);
    Equal("TR_A1b2C3d4", mute.RootElement.GetProperty("trackSid").GetString() ?? string.Empty);
    True(mute.RootElement.GetProperty("muted").GetBoolean());
}

static void AdminRejectsUntrustedTargets()
{
    var handler = new CaptureHandler();
    using var http = new HttpClient(handler);
    var admin = new LiveKitAdminClient(ValidAdminOptions(), http);
    Throws<LiveKitAdminException>(() => admin.RemoveParticipantAsync(
        new VoiceControlTarget("bad:room", "steam:76561198000000000", VoiceMode.Coop, null)).GetAwaiter().GetResult());
    Throws<LiveKitAdminException>(() => admin.SetMicrophonePermissionAsync(
        new VoiceControlTarget("beach_001", "steam:76561198000000000", VoiceMode.Pvp, null), false).GetAwaiter().GetResult());
    Throws<LiveKitAdminException>(() => admin.MutePublishedTrackAsync(
        new VoiceControlTarget("beach_001", "steam:76561198000000000", VoiceMode.Coop, null), "../../bad").GetAwaiter().GetResult());
    var disabled = new LiveKitAdminClient(new LiveKitAdminOptions(), http);
    Throws<LiveKitAdminException>(() => disabled.RemoveParticipantAsync(
        new VoiceControlTarget("beach_001", "steam:76561198000000000", VoiceMode.Coop, null)).GetAwaiter().GetResult());
    Equal(0, handler.Requests.Count);
}

static void WebhookVerification()
{
    var now = new DateTimeOffset(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);
    var body = Encoding.UTF8.GetBytes("{\"id\":\"EV_12345678\",\"createdAt\":1789291200,\"event\":\"participant_joined\",\"participant\":{\"identity\":\"steam:1\"}}");
    var token = SignWebhook(body, now, now.AddMinutes(5), "webhook-1");
    var verifier = new LiveKitWebhookVerifier(ValidWebhookOptions(), new FixedTimeProvider(now));
    var result = verifier.Verify(body, token);
    Equal("EV_12345678", result.Id);
    Equal("participant_joined", result.Event);
    Equal("steam:1", result.Payload.GetProperty("participant").GetProperty("identity").GetString() ?? string.Empty);
}

static void WebhookTamperExpiryAndReplay()
{
    var now = new DateTimeOffset(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);
    var body = Encoding.UTF8.GetBytes("{\"id\":\"EV_12345678\",\"createdAt\":1789291200,\"event\":\"track_published\"}");
    var verifier = new LiveKitWebhookVerifier(ValidWebhookOptions(), new FixedTimeProvider(now));
    var token = SignWebhook(body, now, now.AddMinutes(5), "webhook-replay");
    verifier.Verify(body, "Bearer " + token);
    Throws<LiveKitWebhookException>(() => verifier.Verify(body, token));

    var tampered = Encoding.UTF8.GetBytes("{\"id\":\"EV_12345678\",\"createdAt\":1789291200,\"event\":\"track_unpublished\"}");
    Throws<LiveKitWebhookException>(() => new LiveKitWebhookVerifier(ValidWebhookOptions(), new FixedTimeProvider(now)).Verify(tampered, token));

    var expired = SignWebhook(body, now.AddMinutes(-10), now.AddMinutes(-5), "webhook-expired");
    Throws<LiveKitWebhookException>(() => new LiveKitWebhookVerifier(ValidWebhookOptions(), new FixedTimeProvider(now)).Verify(body, expired));

    var pieces = token.Split('.');
    var badSignature = pieces[2][0] == 'A' ? "B" + pieces[2][1..] : "A" + pieces[2][1..];
    Throws<LiveKitWebhookException>(() => new LiveKitWebhookVerifier(ValidWebhookOptions(), new FixedTimeProvider(now)).Verify(body, $"{pieces[0]}.{pieces[1]}.{badSignature}"));
}

static LiveKitAdminOptions ValidAdminOptions() => new()
{
    Enabled = true,
    ServerHttpUrl = "http://127.0.0.1:7880",
    ApiKey = "sandflow-key",
    ApiSecret = "0123456789abcdef0123456789abcdef",
};

static LiveKitWebhookOptions ValidWebhookOptions() => new()
{
    Enabled = true,
    ApiKey = "sandflow-key",
    ApiSecret = "0123456789abcdef0123456789abcdef",
};

static string SignWebhook(byte[] body, DateTimeOffset issuedAt, DateTimeOffset expiresAt, string jti)
{
    var header = Base64Url(JsonSerializer.SerializeToUtf8Bytes(new { alg = "HS256", typ = "JWT" }));
    var payload = Base64Url(JsonSerializer.SerializeToUtf8Bytes(new
    {
        iss = "sandflow-key",
        iat = issuedAt.ToUnixTimeSeconds(),
        nbf = issuedAt.ToUnixTimeSeconds(),
        exp = expiresAt.ToUnixTimeSeconds(),
        jti,
        sha256 = Convert.ToBase64String(SHA256.HashData(body)),
    }));
    var unsigned = $"{header}.{payload}";
    using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes("0123456789abcdef0123456789abcdef"));
    return $"{unsigned}.{Base64Url(hmac.ComputeHash(Encoding.ASCII.GetBytes(unsigned)))}";
}

static JsonDocument ReadPayload(string jwt)
{
    var payload = jwt.Split('.')[1].Replace('-', '+').Replace('_', '/');
    payload = payload.PadRight(payload.Length + ((4 - payload.Length % 4) % 4), '=');
    return JsonDocument.Parse(Convert.FromBase64String(payload));
}

static string Base64Url(byte[] value) => Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');
static void True(bool value) { if (!value) throw new Exception("Expected true."); }
static void False(bool value) { if (value) throw new Exception("Expected false."); }
static void Equal<T>(T expected, T actual) where T : notnull
{
    if (!EqualityComparer<T>.Default.Equals(expected, actual))
        throw new Exception($"Expected '{expected}', got '{actual}'.");
}
static void Throws<T>(Action action) where T : Exception
{
    try { action(); }
    catch (T) { return; }
    throw new Exception($"Expected {typeof(T).Name}.");
}

sealed class FixedTimeProvider(DateTimeOffset instant) : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => instant;
}

sealed record CapturedRequest(string Path, string Body, string BearerToken);

sealed class CaptureHandler : HttpMessageHandler
{
    public List<CapturedRequest> Requests { get; } = [];

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken);
        Requests.Add(new CapturedRequest(
            request.RequestUri?.AbsolutePath ?? string.Empty,
            body,
            request.Headers.Authorization?.Parameter ?? string.Empty));
        return new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("{}", Encoding.UTF8, "application/json"),
        };
    }
}
