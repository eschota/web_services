using Microsoft.Data.Sqlite;
using System.Security.Cryptography;
using System.Text.Json;

namespace SandFlow.Server;

public sealed class WorldStore
{
    private readonly string _root;
    private readonly string _connection;
    private readonly object _gate = new();
    public WorldStore(string root)
    {
        _root = Path.GetFullPath(root);
        Directory.CreateDirectory(_root);
        Directory.CreateDirectory(Path.Combine(_root, "snapshots"));
        _connection = new SqliteConnectionStringBuilder { DataSource = Path.Combine(_root, "sandflow.db") }.ToString();
        using var db = Open();
        Execute(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;" +
            "CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY, identity_json TEXT NOT NULL, expires INTEGER NOT NULL);" +
            "CREATE TABLE IF NOT EXISTS worlds(id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, private INTEGER NOT NULL, json TEXT NOT NULL);" +
            "CREATE TABLE IF NOT EXISTS snapshots(world_id TEXT NOT NULL, revision INTEGER NOT NULL, json TEXT NOT NULL, PRIMARY KEY(world_id,revision));" +
            "CREATE TABLE IF NOT EXISTS blocks(world_id TEXT NOT NULL, player_id TEXT NOT NULL, PRIMARY KEY(world_id,player_id));");
    }
    private SqliteConnection Open() { var db = new SqliteConnection(_connection); db.Open(); return db; }
    private static void Execute(SqliteConnection db, string sql)
    { using var cmd = db.CreateCommand(); cmd.CommandText = sql; cmd.ExecuteNonQuery(); }
    public SessionGrant NewGuest(DateTimeOffset now)
    {
        var token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
        var identity = new Identity(Protocol.RandomId(), false, null);
        using var db = Open(); using var cmd = db.CreateCommand();
        cmd.CommandText = "INSERT INTO sessions VALUES($hash,$json,$expires)";
        cmd.Parameters.AddWithValue("$hash", Protocol.Hash(token));
        cmd.Parameters.AddWithValue("$json", JsonSerializer.Serialize(identity));
        cmd.Parameters.AddWithValue("$expires", now.AddDays(30).ToUnixTimeSeconds()); cmd.ExecuteNonQuery();
        return new(token, identity);
    }
    public Identity? Authenticate(string? token, DateTimeOffset now)
    {
        if (token == null || token.Length != 64 || !token.All(Uri.IsHexDigit)) return null;
        using var db = Open(); using var cmd = db.CreateCommand();
        cmd.CommandText = "SELECT identity_json FROM sessions WHERE token_hash=$hash AND expires>$now";
        cmd.Parameters.AddWithValue("$hash", Protocol.Hash(token)); cmd.Parameters.AddWithValue("$now", now.ToUnixTimeSeconds());
        return cmd.ExecuteScalar() is string json ? JsonSerializer.Deserialize<Identity>(json) : null;
    }
    public WorldRecord Create(Identity owner, CreateWorld request, DateTimeOffset now)
    {
        if (request.Mode is not ("coop" or "pvp") || request.TeamSize is < 1 or > 4)
            throw new ApiFailure(400, "invalid_mode");
        if (!request.Demo && !owner.FullAccess) throw new ApiFailure(403, "full_access_required");
        if (!Protocol.DemoMaps.Contains(request.Map)) throw new ApiFailure(400, "map_not_enabled_yet");
        if (request.Demo && request.TeamSize != 1) throw new ApiFailure(403, "demo_team_limit");
        string? password = request.IsPrivate ? Passwords.Create(request.Password ?? "") : null;
        var capacity = request.Mode == "pvp" ? request.TeamSize * 2 : request.Demo ? 4 : 16;
        var world = new WorldRecord(Protocol.RandomId(), owner.Id, request.Mode, request.Map, request.IsPrivate,
            password, request.Demo, capacity, request.TeamSize, now.ToUnixTimeSeconds());
        lock (_gate)
        {
            using var db = Open(); using var count = db.CreateCommand();
            count.CommandText = "SELECT count(*) FROM worlds WHERE owner_id=$owner";
            count.Parameters.AddWithValue("$owner", owner.Id);
            if ((long)count.ExecuteScalar()! >= (owner.FullAccess ? 10 : 2)) throw new ApiFailure(429, "world_quota");
            using var cmd = db.CreateCommand(); cmd.CommandText = "INSERT INTO worlds VALUES($id,$owner,$private,$json)";
            cmd.Parameters.AddWithValue("$id", world.Id); cmd.Parameters.AddWithValue("$owner", owner.Id);
            cmd.Parameters.AddWithValue("$private", world.IsPrivate ? 1 : 0); cmd.Parameters.AddWithValue("$json", JsonSerializer.Serialize(world));
            cmd.ExecuteNonQuery();
        }
        return world;
    }
    public WorldRecord Find(string id)
    {
        if (!Protocol.ValidId(id)) throw new ApiFailure(404, "world_not_found");
        using var db = Open(); using var cmd = db.CreateCommand(); cmd.CommandText = "SELECT json FROM worlds WHERE id=$id";
        cmd.Parameters.AddWithValue("$id", id);
        return cmd.ExecuteScalar() is string json ? JsonSerializer.Deserialize<WorldRecord>(json)! : throw new ApiFailure(404, "world_not_found");
    }
    public List<WorldRecord> PublicWorlds()
    {
        using var db = Open(); using var cmd = db.CreateCommand(); cmd.CommandText = "SELECT json FROM worlds WHERE private=0 ORDER BY rowid DESC LIMIT 200";
        using var reader = cmd.ExecuteReader(); var worlds = new List<WorldRecord>();
        while (reader.Read()) worlds.Add(JsonSerializer.Deserialize<WorldRecord>(reader.GetString(0))!);
        return worlds;
    }
    public bool IsBlocked(string world, string player)
    {
        using var db = Open(); using var cmd = db.CreateCommand(); cmd.CommandText = "SELECT 1 FROM blocks WHERE world_id=$w AND player_id=$p";
        cmd.Parameters.AddWithValue("$w", world); cmd.Parameters.AddWithValue("$p", player); return cmd.ExecuteScalar() != null;
    }
    public void Block(string world, string player)
    {
        using var db = Open(); using var cmd = db.CreateCommand(); cmd.CommandText = "INSERT OR IGNORE INTO blocks VALUES($w,$p)";
        cmd.Parameters.AddWithValue("$w", world); cmd.Parameters.AddWithValue("$p", player); cmd.ExecuteNonQuery();
    }
    public List<SnapshotRecord> Versions(string world)
    {
        using var db = Open(); using var cmd = db.CreateCommand(); cmd.CommandText = "SELECT json FROM snapshots WHERE world_id=$w ORDER BY revision DESC LIMIT 10";
        cmd.Parameters.AddWithValue("$w", world); using var reader = cmd.ExecuteReader(); var result = new List<SnapshotRecord>();
        while (reader.Read()) result.Add(JsonSerializer.Deserialize<SnapshotRecord>(reader.GetString(0))!);
        return result;
    }
    public byte[] Read(SnapshotRecord record)
    {
        var bytes = File.ReadAllBytes(Path.Combine(_root, "snapshots", record.Sha256 + ".sfs"));
        if (bytes.Length != record.Bytes || Convert.ToHexString(SHA256.HashData(bytes)) != record.Sha256)
            throw new ApiFailure(503, "snapshot_integrity_failure");
        return bytes;
    }
    public SnapshotRecord Save(string world, SnapshotUpload request, byte[] data, DateTimeOffset now)
    {
        if (data.Length is < 16 or > Protocol.MaxSnapshotBytes) throw new ApiFailure(413, "snapshot_size");
        // Opaque payload is not decompressed by the relay; client format validation is a separate gate.
        var hash = Convert.ToHexString(SHA256.HashData(data));
        if (!hash.Equals(request.Sha256, StringComparison.OrdinalIgnoreCase)) throw new ApiFailure(400, "snapshot_hash");
        lock (_gate)
        {
            var previous = Versions(world).FirstOrDefault();
            if ((previous?.Revision ?? 0) != request.ExpectedRevision) throw new ApiFailure(409, "snapshot_revision_conflict");
            var record = new SnapshotRecord(world, request.ExpectedRevision + 1, request.Epoch, request.Tick, request.Sequence, hash, data.Length, now.ToUnixTimeSeconds());
            var path = Path.Combine(_root, "snapshots", hash + ".sfs");
            if (!File.Exists(path))
            {
                var temp = path + "." + Protocol.RandomId() + ".pending";
                using (var stream = new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                { stream.Write(data); stream.Flush(true); }
                File.Move(temp, path, false);
            }
            using var db = Open(); using var tx = db.BeginTransaction(); using var cmd = db.CreateCommand();
            cmd.Transaction = tx; cmd.CommandText = "INSERT INTO snapshots VALUES($w,$rev,$json); DELETE FROM snapshots WHERE world_id=$w AND revision <= $rev-10;";
            cmd.Parameters.AddWithValue("$w", world); cmd.Parameters.AddWithValue("$rev", record.Revision);
            cmd.Parameters.AddWithValue("$json", JsonSerializer.Serialize(record)); cmd.ExecuteNonQuery(); tx.Commit();
            return record;
        }
    }
}
