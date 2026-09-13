using Microsoft.Data.Sqlite;
using System.Text.Json;

namespace SandFlow.Server;

/// <summary>Client-reported diagnostics, never authoritative hardware benchmark or identity.</summary>
public sealed record DeviceTelemetry(int SchemaVersion, string SessionId, string AppVersion, string Platform,
    string Renderer, string GpuModel, int GraphicsMemoryMB, int SystemMemoryMB, int LogicalProcessorCount,
    int Width, int Height, int QualityMode, bool AutoQuality, double IntervalSeconds, double MeanFps,
    double P95FrameMs, int FrameCount);

public sealed class DeviceTelemetryStore
{
    private readonly string _connection;
    private readonly TimeProvider _time;
    private readonly object _gate = new();
    private long _windowSecond;
    private int _windowCount;
    public DeviceTelemetryStore(string root, TimeProvider time)
    {
        Directory.CreateDirectory(root); _time = time;
        _connection = new SqliteConnectionStringBuilder { DataSource = Path.Combine(root, "telemetry.db") }.ToString();
        using var db = Open(); using var command = db.CreateCommand();
        command.CommandText = "PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS device_reports(session_id TEXT PRIMARY KEY, received INTEGER NOT NULL, quality INTEGER NOT NULL, mean_fps REAL NOT NULL, json TEXT NOT NULL);";
        command.ExecuteNonQuery();
    }
    private SqliteConnection Open() { var connection = new SqliteConnection(_connection); connection.Open(); return connection; }
    public static void Validate(DeviceTelemetry report)
    {
        if (report.SchemaVersion != 1 || report.SessionId == null || !Protocol.ValidId(report.SessionId)) throw new ApiFailure(400, "telemetry_schema");
        foreach (var text in new[] { report.AppVersion, report.Platform, report.Renderer, report.GpuModel })
            if (text == null || text.Length > 120 || text.Any(char.IsControl)) throw new ApiFailure(400, "telemetry_text");
        if (report.GraphicsMemoryMB is < 0 or > 1048576 || report.SystemMemoryMB is < 0 or > 4194304 ||
            report.LogicalProcessorCount is < 0 or > 1024 || report.Width is < 1 or > 32768 || report.Height is < 1 or > 32768 ||
            report.QualityMode is < 1 or > 3 || report.FrameCount is < 1 or > 1000000 ||
            !double.IsFinite(report.IntervalSeconds) || report.IntervalSeconds is < 1 or > 300 ||
            !double.IsFinite(report.MeanFps) || report.MeanFps is < 0 or > 2000 ||
            !double.IsFinite(report.P95FrameMs) || report.P95FrameMs is < 0 or > 30000)
            throw new ApiFailure(400, "telemetry_range");
    }
    public void Accept(DeviceTelemetry report)
    {
        Validate(report);
        lock (_gate)
        {
            var now = _time.GetUtcNow().ToUnixTimeSeconds();
            if (_windowSecond != now) { _windowSecond = now; _windowCount = 0; }
            if (++_windowCount > 10) throw new ApiFailure(429, "telemetry_capacity");
            using var db = Open(); using var check = db.CreateCommand();
            check.CommandText = "SELECT received FROM device_reports WHERE session_id=$id";
            check.Parameters.AddWithValue("$id", report.SessionId);
            if (check.ExecuteScalar() is long previous && now - previous < 15) throw new ApiFailure(429, "telemetry_interval");
            using var tx = db.BeginTransaction(); using var command = db.CreateCommand(); command.Transaction = tx;
            command.CommandText = "INSERT INTO device_reports VALUES($id,$now,$quality,$fps,$json) ON CONFLICT(session_id) DO UPDATE SET received=excluded.received,quality=excluded.quality,mean_fps=excluded.mean_fps,json=excluded.json;" +
                "DELETE FROM device_reports WHERE received < $expiry;" +
                "DELETE FROM device_reports WHERE session_id IN (SELECT session_id FROM device_reports ORDER BY received DESC LIMIT -1 OFFSET 5000);";
            command.Parameters.AddWithValue("$id", report.SessionId); command.Parameters.AddWithValue("$now", now);
            command.Parameters.AddWithValue("$quality", report.QualityMode); command.Parameters.AddWithValue("$fps", report.MeanFps);
            command.Parameters.AddWithValue("$json", JsonSerializer.Serialize(report, Wire.Json)); command.Parameters.AddWithValue("$expiry", now - 7 * 86400);
            command.ExecuteNonQuery(); tx.Commit();
        }
    }
    public int Count()
    { using var db = Open(); using var command = db.CreateCommand(); command.CommandText = "SELECT count(*) FROM device_reports"; return checked((int)(long)command.ExecuteScalar()!); }
}
