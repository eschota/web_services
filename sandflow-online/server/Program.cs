using System.Net.WebSockets;
using System.Text.Json;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.AspNetCore.HttpOverrides;
using System.Net;
using SandFlow.Server;
using SandFlow.Voice;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<ForwardedHeadersOptions>(options =>
{
    options.ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
    options.ForwardLimit = 1;
    options.KnownProxies.Clear();
    options.KnownProxies.Add(IPAddress.Loopback);
});
builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = Protocol.MaxSnapshotBytes);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton(new WorldStore(builder.Configuration["SANDFLOW_DATA"] ?? Path.Combine(AppContext.BaseDirectory, "data")));
builder.Services.AddSingleton(new DeviceTelemetryStore(builder.Configuration["SANDFLOW_DATA"] ?? Path.Combine(AppContext.BaseDirectory, "data"), TimeProvider.System));
builder.Services.AddSingleton<Rooms>();
builder.Services.AddSingleton<QaBrowserAccess>();
builder.Services.AddSingleton(new VoiceTokenService(VoiceTokenOptions.FromEnvironment()));
builder.Services.AddHostedService<RoomHousekeeping>();
builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = 429;
    options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(context =>
        RateLimitPartition.GetTokenBucketLimiter(context.Connection.RemoteIpAddress?.ToString() ?? "unknown",
        _ => new TokenBucketRateLimiterOptions { TokenLimit = 40, TokensPerPeriod = 20, ReplenishmentPeriod = TimeSpan.FromSeconds(1),
            AutoReplenishment = true, QueueLimit = 0 }));
});
var app = builder.Build();
var revisionFile = Path.Combine(AppContext.BaseDirectory, "REVISION");
var buildRevision = File.Exists(revisionFile) ? File.ReadAllText(revisionFile).Trim() : "local";
app.UseForwardedHeaders();
app.Use(async (context, next) =>
{
    if (context.Request.Path == "/api/v1/qa/approve")
    {
        var limit = context.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpMaxRequestBodySizeFeature>();
        if (limit != null && !limit.IsReadOnly) limit.MaxRequestBodySize = 1024;
    }
    context.Response.Headers.CacheControl = "no-store";
    context.Response.Headers.XContentTypeOptions = "nosniff";
    context.Response.Headers["Referrer-Policy"] = "no-referrer";
    try
    {
        // The nginx proxy is the only public listener. Never trust arbitrary forwarded identity.
        if (context.Request.Headers.TryGetValue("Origin", out var origin) && origin != "https://autorig.online")
            throw new ApiFailure(403, "origin_denied");
        await next(context);
    }
    catch (ApiFailure failure) when (!context.Response.HasStarted)
    { context.Response.StatusCode = failure.Status; await context.Response.WriteAsJsonAsync(new { error = failure.Code }); }
    catch (VoiceTokenException) when (!context.Response.HasStarted)
    { context.Response.StatusCode = 503; await context.Response.WriteAsJsonAsync(new { error = "voice_unavailable" }); }
    catch (JsonException) when (!context.Response.HasStarted)
    { context.Response.StatusCode = 400; await context.Response.WriteAsJsonAsync(new { error = "invalid_json" }); }
});
app.UseRateLimiter();
app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(15) });
app.MapGet("/health", () => Results.Ok(new { service = "sandflow", protocol = Protocol.Version, physics = "client-only",
    admissionEnabled = builder.Configuration["SANDFLOW_ADMISSION_ENABLED"] == "true", stage = "foundation-not-gameplay-validated", revision = buildRevision }));
var api = app.MapGroup("/api/v1");
api.MapGet("/qa", (HttpContext context, QaBrowserAccess access) =>
{
    if (string.IsNullOrEmpty(builder.Configuration["SANDFLOW_QA_TOKEN"])) throw new ApiFailure(404, "qa_disabled");
    var challenge = access.Begin(context.Request.Cookies["sf_qa_challenge"]);
    context.Response.Cookies.Append("sf_qa_challenge", challenge.CookieValue, new CookieOptions
    { HttpOnly = true, Secure = true, SameSite = SameSiteMode.Strict, Path = "/sandflow/api/v1/qa", Expires = challenge.Expires });
    context.Response.Headers["Content-Security-Policy"] = "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'";
    context.Response.Headers["X-Robots-Tag"] = "noindex, nofollow, noarchive";
    var status = challenge.Approved ? "Approved. Continue to the assigned test sandbox." : "Waiting for QA authorization. This code alone grants no access.";
    return Results.Content("<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"><title>SandFlow browser QA access</title>" +
        "<style>body{font:18px system-ui;background:#15202a;color:#e5f4fa;max-width:640px;margin:10vh auto;padding:24px}code{display:block;padding:18px;background:#263947;overflow-wrap:anywhere}button{padding:14px;margin-top:24px}</style>" +
        "<h1>SandFlow browser QA access</h1><p>Private validation session — not a public release.</p><p>Browser challenge:</p><code id=\"qa-code\">" + challenge.Code +
        "</code><p>" + status + "</p><p>Expires: " + challenge.Expires.ToString("u") + ". No access key belongs in the URL.</p><form method=\"post\" action=\"/sandflow/api/v1/qa/claim\"><button type=\"submit\">Continue to sandbox</button></form></html>", "text/html; charset=utf-8");
});
api.MapPost("/qa/approve", (HttpContext context, QaApproval request, QaBrowserAccess access, WorldStore store) =>
{
    var authorized = AdmissionPolicy.Allows(false, false, builder.Configuration["SANDFLOW_QA_TOKEN"] ?? "", context.Request.Headers["X-SF-QA-Token"].ToString());
    if (!authorized) throw new ApiFailure(403, "qa_authority_required");
    if (request.WorldId == null || !Protocol.ValidId(request.WorldId)) throw new ApiFailure(400, "qa_world");
    var world = store.Find(request.WorldId);
    if (!world.Demo || world.IsPrivate) throw new ApiFailure(403, "qa_public_demo_required");
    access.Approve(request.Code, world.Id, true); return Results.Ok(new { approved = true });
});
api.MapPost("/qa/claim", (HttpContext context, QaBrowserAccess access, WorldStore store) =>
{
    if (string.IsNullOrEmpty(builder.Configuration["SANDFLOW_QA_TOKEN"])) throw new ApiFailure(404, "qa_disabled");
    var claim = access.Claim(context.Request.Cookies["sf_qa_challenge"], () => store.NewGuest(DateTimeOffset.UtcNow, true, TimeSpan.FromHours(2)));
    context.Response.Cookies.Append("sf_session", claim.Session.Token, new CookieOptions
    { HttpOnly = true, Secure = true, SameSite = SameSiteMode.Strict, Path = "/sandflow/", MaxAge = TimeSpan.FromHours(2) });
    return Results.Redirect("/sandflow/qa/s/" + claim.WorldId);
});
api.MapPost("/telemetry", async (HttpContext context, DeviceTelemetryStore store) =>
{
    // Explicit bounded body read before deserialization; no arbitrary diagnostic strings or IP/Steam identifiers.
    if (context.Request.ContentLength > 8192) throw new ApiFailure(413, "telemetry_size");
    var buffer = new byte[8193]; var length = 0;
    while (length < buffer.Length)
    {
        var count = await context.Request.Body.ReadAsync(buffer.AsMemory(length), context.RequestAborted);
        if (count == 0) break;
        length += count;
    }
    if (length > 8192) throw new ApiFailure(413, "telemetry_size");
    var report = JsonSerializer.Deserialize<DeviceTelemetry>(buffer.AsSpan(0, length), Wire.Json) ?? throw new ApiFailure(400, "telemetry_empty");
    store.Accept(report); return Results.Accepted();
});
Identity Auth(HttpContext context, WorldStore store)
{
    var authorization = context.Request.Headers.Authorization.ToString();
    var token = authorization.StartsWith("Bearer ", StringComparison.Ordinal) ? authorization[7..] : context.Request.Cookies["sf_session"];
    return store.Authenticate(token, DateTimeOffset.UtcNow) ?? throw new ApiFailure(401, "authentication_required");
}
void AdmissionGate(HttpContext context, Identity? identity = null)
{
    if (!AdmissionPolicy.Allows(builder.Configuration["SANDFLOW_ADMISSION_ENABLED"] == "true", identity?.PreviewApproved == true,
        builder.Configuration["SANDFLOW_QA_TOKEN"] ?? "", context.Request.Headers["X-SF-QA-Token"].ToString()))
        throw new ApiFailure(503, "online_preview_not_ready");
}
api.MapPost("/sessions/guest", (HttpContext context, WorldStore store) =>
{
    var existing = context.Request.Cookies["sf_session"];
    var identity = store.Authenticate(existing, DateTimeOffset.UtcNow);
    AdmissionGate(context, identity);
    if (identity != null) return Results.Ok(new { token = existing, identity });
    var grant = store.NewGuest(DateTimeOffset.UtcNow, builder.Configuration["SANDFLOW_ADMISSION_ENABLED"] != "true");
    context.Response.Cookies.Append("sf_session", grant.Token, new CookieOptions
    { HttpOnly = true, Secure = true, SameSite = SameSiteMode.Strict, Path = "/sandflow/", MaxAge = TimeSpan.FromDays(30) });
    return Results.Ok(new { token = grant.Token, identity = grant.Identity });
});
api.MapGet("/worlds", (WorldStore store, Rooms rooms) => Results.Ok(store.PublicWorlds().Select(w => rooms.View(w.Id))));
api.MapPost("/worlds", (HttpContext context, CreateWorld request, WorldStore store, Rooms rooms) =>
{
    var owner = Auth(context, store); AdmissionGate(context, owner); var world = store.Create(owner, request, DateTimeOffset.UtcNow);
    return Results.Ok(rooms.Join(owner, world.Id, null));
});
api.MapPost("/worlds/autojoin", (HttpContext context, WorldStore store, Rooms rooms) =>
{ var identity = Auth(context, store); AdmissionGate(context, identity); return Results.Ok(rooms.AutoJoin(identity)); });
api.MapPost("/worlds/{id}/join", (HttpContext context, string id, JoinRequest request, WorldStore store, Rooms rooms) =>
{ var identity = Auth(context, store); AdmissionGate(context, identity); return Results.Ok(rooms.Join(identity, id, request.Password)); });
api.MapGet("/worlds/{id}/snapshots", (HttpContext context, string id, WorldStore store, Rooms rooms) =>
{ rooms.Member(Auth(context, store), id); return Results.Ok(store.Versions(id)); });
api.MapGet("/worlds/{id}/snapshots/latest", (HttpContext context, string id, WorldStore store, Rooms rooms) =>
{ var record = rooms.Latest(Auth(context, store), id); return Results.Bytes(store.Read(record), "application/octet-stream"); });
api.MapPost("/worlds/{id}/snapshots", async (HttpContext context, string id, WorldStore store, Rooms rooms) =>
{
    var identity = Auth(context, store); rooms.Member(identity, id);
    long Header(string key) => long.TryParse(context.Request.Headers[key], out var n) && n >= 0 ? n : throw new ApiFailure(400, "snapshot_header");
    var upload = new SnapshotUpload(Header("X-SF-Epoch"), Header("X-SF-Tick"), Header("X-SF-Sequence"),
        Header("X-SF-Revision"), context.Request.Headers["X-SF-SHA256"].ToString());
    if (context.Request.ContentLength is null or > Protocol.MaxSnapshotBytes or < 16) throw new ApiFailure(413, "snapshot_size");
    using var body = new MemoryStream((int)context.Request.ContentLength.Value);
    await context.Request.Body.CopyToAsync(body, context.RequestAborted);
    return Results.Ok(rooms.Save(identity, id, upload, body.ToArray(), context.Request.Headers["X-SF-Final"] == "true"));
});
api.MapPost("/worlds/{id}/kick/{player}", (HttpContext context, string id, string player, WorldStore store, Rooms rooms) =>
{ rooms.Kick(Auth(context, store), id, player); return Results.NoContent(); });
api.MapPost("/worlds/{id}/voice-token", (HttpContext context, string id, WorldStore store, Rooms rooms, VoiceTokenService voice) =>
{
    var identity = Auth(context, store);
    lock (rooms.Gate)
    {
        var peer = rooms.Member(identity, id); var view = rooms.View(id);
        if (peer.ConnectionId == null || !peer.Ready) throw new ApiFailure(409, "active_membership_required");
        return Results.Ok(voice.IssueToken(new(id, identity.Id, view.Mode == "pvp" ? VoiceMode.Pvp : VoiceMode.Coop,
            view.Mode == "pvp" ? peer.Team : null, true, true)));
    }
});
// No simulated Steam entitlement: unavailable until official ticket/OpenID verification is implemented.
api.MapPost("/sessions/steam", () => Results.Json(new { error = "steam_auth_not_enabled" }, statusCode: 503));
app.Map("/ws/{id}/{channel}", async (HttpContext context, string id, string channel, WorldStore store, Rooms rooms) =>
{
    var identity = Auth(context, store); AdmissionGate(context, identity);
    if (!context.WebSockets.IsWebSocketRequest || channel is not ("control" or "state")) throw new ApiFailure(400, "websocket_required");
    var bulk = channel == "state";
    var (peer, connection) = rooms.Connect(identity, id, bulk);
    using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
    try
    {
        using var socket = await context.WebSockets.AcceptWebSocketAsync();
        var send = SendLoop(socket, peer, bulk, cancellation.Token);
        var receive = ReceiveLoop(socket, identity, id, connection, bulk, rooms, cancellation.Token);
        var finished = await Task.WhenAny(send, receive);
        if (finished.IsFaulted)
        {
            var failure = finished.Exception?.GetBaseException();
            // Log only our fixed rejection code/type, never message bodies, tokens or passwords.
            var code = failure is ApiFailure rejected ? rejected.Code : failure?.GetType().Name;
            app.Logger.LogWarning("SandFlow socket ended: channel={Channel} code={Code}", channel, code);
        }
        await cancellation.CancelAsync();
        try { await Task.WhenAll(send, receive); } catch (OperationCanceledException) { }
        if (socket.State == WebSocketState.Open) await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "reconnect", CancellationToken.None);
    }
    catch (WebSocketException) { }
    catch (ApiFailure) { /* Invalid control closes the peer. Never echo auth material or exception details. */ }
    finally { rooms.Disconnect(identity, id, connection, bulk); }
});
await app.RunAsync();

static async Task SendLoop(WebSocket socket, RoomPeer peer, bool bulk, CancellationToken cancel)
{
    if (bulk)
        await foreach (var data in peer.Bulk.Reader.ReadAllAsync(cancel)) await socket.SendAsync(data, WebSocketMessageType.Binary, true, cancel);
    else
        await foreach (var message in peer.Events.Reader.ReadAllAsync(cancel))
            await socket.SendAsync(JsonSerializer.SerializeToUtf8Bytes(message, Wire.Json), WebSocketMessageType.Text, true, cancel);
}
static async Task ReceiveLoop(WebSocket socket, Identity identity, string id, string connection, bool bulk, Rooms rooms, CancellationToken cancel)
{
    var limit = bulk ? Protocol.MaxBulkBytes : Protocol.MaxCommandBytes;
    var bytes = new byte[limit];
    while (!cancel.IsCancellationRequested && socket.State == WebSocketState.Open)
    {
        var count = 0; ValueWebSocketReceiveResult result;
        do
        {
            if (count == bytes.Length) throw new ApiFailure(413, "websocket_message_size");
            result = await socket.ReceiveAsync(bytes.AsMemory(count), cancel);
            if (result.MessageType == WebSocketMessageType.Close) return;
            if (result.MessageType != (bulk ? WebSocketMessageType.Binary : WebSocketMessageType.Text)) throw new ApiFailure(400, "websocket_message_type");
            count += result.Count;
        } while (!result.EndOfMessage);
        if (bulk) rooms.RelayBulk(identity, id, connection, bytes.AsSpan(0, count).ToArray());
        else rooms.Receive(identity, id, connection, JsonSerializer.Deserialize<ControlMessage>(bytes.AsSpan(0, count), Wire.Json)
            ?? throw new ApiFailure(400, "empty_message"));
    }
}
public sealed record JoinRequest(string? Password = null);
public sealed record QaApproval(string? Code, string? WorldId);
public static class Wire
{
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    { MaxDepth = 8, UnmappedMemberHandling = System.Text.Json.Serialization.JsonUnmappedMemberHandling.Disallow };
}
public sealed class RoomHousekeeping(Rooms rooms) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(stoppingToken)) rooms.Sweep();
    }
}
