using System.Security.Cryptography;
using System.Text;

namespace SandFlow.Server;

public static class Protocol
{
    public const int Version = SandFlow.Protocol.SessionProtocol.Version;
    public const int TileSize = 32;
    public const int MaximumMembers = 100;
    public const int MaxCommandBytes = 16 * 1024;
    public const int MaxSnapshotBytes = 64 * 1024 * 1024;
    public const int MaxBulkBytes = 1024 * 1024;
    public static readonly string[] DemoMaps = ["gentle-beach", "twin-shore-river", "sandbox-flat"];
    public static string RandomId() => Guid.NewGuid().ToString("N");
    public static bool ValidId(string id) => id.Length == 32 && id.All(Uri.IsHexDigit);
    public static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
}

public sealed class ApiFailure(int status, string code) : Exception(code)
{
    public int Status { get; } = status;
    public string Code { get; } = code;
}

public sealed record Identity(string Id, bool FullAccess, string? SteamId, bool PreviewApproved = false);
public sealed record SessionGrant(string Token, Identity Identity);
public sealed record CreateWorld(string Mode = "coop", string Map = "twin-shore-river", bool IsPrivate = false,
    string? Password = null, bool Demo = true, int TeamSize = 1);
public sealed record WorldRecord(string Id, string OwnerId, string Mode, string Map, bool IsPrivate,
    string? PasswordHash, bool Demo, int Capacity, int TeamSize, long CreatedUnix);
public sealed record WorldView(string Id, string Mode, string Map, bool IsPrivate, bool Demo, int Capacity,
    int Occupancy, string State, long Revision);
public sealed record SnapshotRecord(string WorldId, long Revision, long Epoch, long Tick, long Sequence,
    string Sha256, int Bytes, long CreatedUnix);
public sealed record SnapshotUpload(long Epoch, long Tick, long Sequence, long ExpectedRevision, string Sha256);
public sealed record Admission(WorldView World, string ParticipantId, int Team, int ProtocolVersion);
public sealed record ControlMessage(string Type, int Version = Protocol.Version, long Epoch = 0, long Tick = 0,
    long Sequence = 0, int Substeps = 1, WorldCommand? Command = null, CommitRecord[]? Commits = null, string? ParticipantId = null, string? OperationId = null, string? TuningDefaultsHash = null);
public sealed record DepartureMarker(string OperationId, string ParticipantId);
public sealed record DepartureReceipt(string OperationId, string ParticipantId, long ExpectedRevision, SnapshotRecord Snapshot);
public sealed record DepartureState(string OperationId, string ParticipantId, long Epoch, long Tick, long Sequence, DateTimeOffset Deadline);
public sealed record TuningChange(string Key, float Value);
public sealed record WorldCommand(string Kind, string Action, float X = 0, float Z = 0, float Radius = 0,
    float Amount = 0, string? Key = null, float Value = 0, string? ObjectId = null, uint CommandType = 0,
    uint Flags = 0, uint MaterialId = 0, float Strength = 0, float Duration = 0,
    float Y = 0, float Qx = 0, float Qy = 0, float Qz = 0, float Qw = 1, TuningChange[]? Settings = null);
public sealed record OrderedCommand(long Sequence, string ParticipantId, WorldCommand Command, long ClientSequence = 0);
public sealed record ServerEvent(string Type, string WorldId, long Epoch, long Tick, long Sequence,
    object? Data = null);

public static class CommandValidation
{
    // Only command families the current client can replay. Planned families must not
    // be broadcast as apparently-valid input that pauses every client on receipt.
    private static readonly HashSet<string> Kinds = ["simulation", "draggable", "setting", "world-option", "map", "reset"];
    public static void Validate(WorldCommand value)
    {
        if (!Kinds.Contains(value.Kind) || string.IsNullOrWhiteSpace(value.Action) || value.Action.Length > 40)
            throw new ApiFailure(400, "invalid_command");
        if (value.Kind == "simulation" && value.Action != "enqueue") throw new ApiFailure(400,"invalid_command");
        foreach (var number in new[] { value.X, value.Y, value.Z, value.Radius, value.Amount, value.Value, value.Strength, value.Duration, value.Qx, value.Qy, value.Qz, value.Qw })
            if (!float.IsFinite(number)) throw new ApiFailure(400, "non_finite_command");
        if (Math.Abs(value.X) > 120 || Math.Abs(value.Y) > 120 || Math.Abs(value.Z) > 120 || value.Radius < 0 || value.Radius > 120 ||
            Math.Abs(value.Amount) > 10000 || Math.Abs(value.Value) > 100000 || Math.Abs(value.Strength) > 10000 || value.Duration is < 0 or > 60 || value.CommandType >= 32 || value.Key?.Length > 80 ||
            value.ObjectId?.Length > 64) throw new ApiFailure(400, "command_out_of_range");
        if (value.Kind == "draggable")
        {
            if (value.ObjectId == null || !Protocol.ValidId(value.ObjectId) ||
                value.Action is not ("pose" or "begin" or "end" or "release" or "settle") || value.Flags > 3 || Math.Abs(value.Amount) > 120)
                throw new ApiFailure(400, "invalid_actor_action");
            var norm = (double)value.Qx * value.Qx + (double)value.Qy * value.Qy + (double)value.Qz * value.Qz + (double)value.Qw * value.Qw;
            if (norm < .99 || norm > 1.01) throw new ApiFailure(400, "invalid_actor_rotation");
        }
        if (value.Kind is "setting" or "world-option")
        {
            if (value.Action != "set" || value.Settings == null || value.Settings.Length < 1
                || value.Settings.Length > (value.Kind=="setting"?SandFlow.Protocol.TuningSettings.SharedCount:SandFlow.Protocol.WorldOptions.All.Count))
                throw new ApiFailure(400, "invalid_tuning_batch");
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var change in value.Settings)
            {
                if (change == null || change.Key == null || !seen.Add(change.Key)) throw new ApiFailure(400, "invalid_tuning_batch");
                try { if(value.Kind=="setting")SandFlow.Protocol.TuningSettings.ValidateShared(change.Key, change.Value);else SandFlow.Protocol.WorldOptions.Validate(change.Key,change.Value); }
                catch (InvalidDataException) { throw new ApiFailure(400, "invalid_tuning_value"); }
            }
        }
        else if (value.Settings is { Length: > 0 }) throw new ApiFailure(400, "unexpected_tuning_batch");
    }
}

public static class Passwords
{
    private const int Iterations = 600000;
    public static string Create(string password)
    {
        if (password.Length is < 8 or > 128) throw new ApiFailure(400, "password_length");
        var salt = RandomNumberGenerator.GetBytes(16);
        var hash = Rfc2898DeriveBytes.Pbkdf2(password, salt, Iterations, HashAlgorithmName.SHA256, 32);
        return $"pbkdf2-sha256${Iterations}${Convert.ToBase64String(salt)}${Convert.ToBase64String(hash)}";
    }
    public static bool Verify(string password, string? encoded)
    {
        if (password.Length > 128 || encoded == null) return false;
        var parts = encoded.Split('$');
        if (parts.Length != 4 || parts[0] != "pbkdf2-sha256" || !int.TryParse(parts[1], out var rounds) || rounds != Iterations)
            return false;
        try
        {
            var actual = Rfc2898DeriveBytes.Pbkdf2(password, Convert.FromBase64String(parts[2]), rounds, HashAlgorithmName.SHA256, 32);
            return CryptographicOperations.FixedTimeEquals(actual, Convert.FromBase64String(parts[3]));
        }
        catch (FormatException) { return false; }
    }
}
