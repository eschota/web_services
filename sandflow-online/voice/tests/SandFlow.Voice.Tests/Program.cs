using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using SandFlow.Voice;

var tests = new (string Name, Action Run)[]
{
    ("team isolation", TeamIsolation),
    ("unauthorized admission", UnauthorizedAdmission),
    ("microphone-only grants", MicrophoneOnlyGrants),
    ("expiry and signature", ExpiryAndSignature),
    ("invalid configuration", InvalidConfiguration),
    ("invalid membership", InvalidMembership),
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
