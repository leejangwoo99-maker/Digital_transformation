using System.Data;
using Npgsql;

namespace AptivDashboard.Gui;

internal sealed record DashboardScope(string ProdDay, string ShiftType)
{
    public string ShiftLabel => ShiftType == "night" ? "야간" : "주간";
}

internal sealed class DatabaseClient
{
    private const string ReportSchema = "i_daily_report";
    private const string GpfSchema = "g_production_film";
    private const string E4PmSchema = "e4_predictive_maintenance";
    private const string DemonSchema = "k_demon_heath_check";

    private readonly string _connectionString;

    private static readonly Dictionary<string, (string DayTable, string NightTable)> ReportTables = new()
    {
        ["final_amount"] = ("a_station_day_daily_final_amount", "a_station_night_daily_final_amount"),
        ["pass_percent"] = ("b_station_day_daily_percentage", "b_station_night_daily_percentage"),
        ["fct_fail_1"] = ("c_1time_step_decription_day_daily", "c_1time_step_decription_night_daily"),
        ["fct_fail_2"] = ("c_2time_step_decription_day_daily", "c_2time_step_decription_night_daily"),
        ["fct_fail_3"] = ("c_3time_over_step_decription_day_daily", "c_3time_over_step_decription_night_daily"),
        ["vision_fail_1"] = ("d_vs_1time_step_decription_day_daily", "d_vs_1time_step_decription_night_daily"),
        ["vision_fail_2"] = ("d_vs_2time_step_decription_day_daily", "d_vs_2time_step_decription_night_daily"),
        ["vision_fail_3"] = ("d_vs_3time_over_step_decription_day_daily", "d_vs_3time_over_step_decription_night_daily"),
        ["worst_case"] = ("f_worst_case_day_daily", "f_worst_case_night_daily"),
        ["afa_wasted"] = ("g_afa_wasted_time_day_daily", "g_afa_wasted_time_night_daily"),
        ["mes_wasted"] = ("h_mes_wasted_time_day_daily", "h_mes_wasted_time_night_daily"),
        ["planned_stop"] = ("i_planned_stop_time_day_daily", "i_planned_stop_time_night_daily"),
        ["non_time"] = ("i_non_time_day_daily", "i_non_time_night_daily"),
        ["oee_line"] = ("k_line_oee_day_daily", "k_line_oee_night_daily"),
        ["oee_station"] = ("k_station_oee_day_daily", "k_station_oee_night_daily"),
        ["oee_total"] = ("k_total_oee_day_daily", "k_total_oee_night_daily"),
        ["mastersample"] = ("e_mastersample_test_day_daily", "e_mastersample_test_night_daily"),
    };

    public DatabaseClient(string databaseUrlOrConnectionString)
    {
        _connectionString = NormalizeConnectionString(databaseUrlOrConnectionString);
    }

    public async Task<string> PingAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            "SELECT current_database()::text || ' / ' || current_user::text",
            connection);
        return Convert.ToString(await command.ExecuteScalarAsync(cancellationToken)) ?? "connected";
    }

    public async Task<DataTable> GetReportAsync(string logicalName, DashboardScope scope, CancellationToken cancellationToken = default)
    {
        if (!ReportTables.TryGetValue(logicalName, out var pair))
        {
            throw new InvalidOperationException($"Unknown report: {logicalName}");
        }

        var table = scope.ShiftType == "night" ? pair.NightTable : pair.DayTable;
        return await QueryByDayAsync(ReportSchema, table, "prod_day", scope.ProdDay, scope.ShiftType, excludeUpdatedAt: true, cancellationToken);
    }

    public async Task<DataTable> GetNonOperationAsync(DashboardScope scope, bool reasonOnly, CancellationToken cancellationToken = default)
    {
        var sql = """
            SELECT
                prod_day::text AS prod_day,
                shift_type::text AS shift_type,
                station::text AS station,
                to_char(from_ts AT TIME ZONE 'Asia/Seoul', 'HH24:MI:SS') AS from_ts,
                to_char(to_ts AT TIME ZONE 'Asia/Seoul', 'HH24:MI:SS') AS to_ts,
                COALESCE(reason::text, '') AS reason,
                COALESCE(sparepart::text, '') AS sparepart
            FROM i_daily_report.total_non_operation_time
            WHERE replace(prod_day::text, '-', '') = @prod_day
              AND lower(shift_type::text) = @shift_type
            """;

        if (reasonOnly)
        {
            sql += " AND COALESCE(btrim(reason::text), '') <> ''";
        }

        sql += " ORDER BY from_ts DESC, station ASC";

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("prod_day", NormalizeDay(scope.ProdDay));
        command.Parameters.AddWithValue("shift_type", NormalizeShift(scope.ShiftType));
        var table = await ExecuteTableAsync(command, cancellationToken);
        MakeEditable(table, "reason", "sparepart");
        return table;
    }

    public async Task<DataTable> GetWorkerInfoAsync(DashboardScope scope, CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT end_day, shift_type, worker_name, order_number
            FROM g_production_film.worker_info
            WHERE end_day = @prod_day AND lower(shift_type) = @shift_type
            ORDER BY worker_name
            """;

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("prod_day", NormalizeDay(scope.ProdDay));
        command.Parameters.AddWithValue("shift_type", NormalizeShift(scope.ShiftType));
        var table = await ExecuteTableAsync(command, cancellationToken);
        MakeEditable(table, "end_day", "shift_type", "worker_name", "order_number");
        return table;
    }

    public async Task<DataTable> GetEmailListAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenAsync(cancellationToken);
        var emailColumn = await GetEmailListColumnAsync(connection, cancellationToken);
        var sql = $"""
            SELECT lower(btrim({emailColumn})) AS email
            FROM g_production_film.email_list
            WHERE {emailColumn} IS NOT NULL AND btrim({emailColumn}) <> ''
            ORDER BY lower(btrim({emailColumn}))
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        return await ExecuteTableAsync(command, cancellationToken);
    }

    public async Task<DataTable> GetRemarkInfoAsync(CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT lower(btrim(barcode_information)) AS barcode_information, pn, remark
            FROM g_production_film.remark_info
            WHERE barcode_information IS NOT NULL AND btrim(barcode_information) <> ''
            ORDER BY lower(btrim(barcode_information))
            """;

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        return await ExecuteTableAsync(command, cancellationToken);
    }

    public async Task<DataTable> GetPlannedTimeAsync(string prodDay, string? shiftType = null, CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT end_day, from_time, to_time, COALESCE(reason, '') AS reason
            FROM g_production_film.planned_time
            WHERE replace(end_day::text, '-', '') = @prod_day
            ORDER BY from_time, to_time
            """;

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("prod_day", NormalizeDay(prodDay));
        var table = await ExecuteTableAsync(command, cancellationToken);
        NormalizePlannedTimeDisplay(table);
        if (!string.IsNullOrWhiteSpace(shiftType))
        {
            table = FilterPlannedTimeByShift(table, NormalizeDay(prodDay), NormalizeShift(shiftType));
        }
        return table;
    }

    public async Task<DataTable> GetRecentAlarmsAsync(string prodDay, int limit = 100, CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT
                end_day::text AS end_day,
                end_time::text AS end_time,
                station::text AS station,
                COALESCE(sparepart::text, '') AS sparepart,
                COALESCE(type_alarm::text, '') AS type_alarm,
                COALESCE(amount::text, '') AS amount
            FROM g_production_film.alarm_record
            WHERE replace(end_day::text, '-', '') = @prod_day
            ORDER BY replace(end_day::text, '-', '') DESC, end_time::time DESC
            LIMIT @limit
            """;

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("prod_day", NormalizeDay(prodDay));
        command.Parameters.AddWithValue("limit", limit);
        return await ExecuteTableAsync(command, cancellationToken);
    }

    public async Task<DataTable> GetPdBoardAsync(string prodDay, CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT end_day, station, last_status, cosine_similarity
            FROM e4_predictive_maintenance.pd_board_check
            WHERE replace(end_day::text, '-', '') = @prod_day
            ORDER BY station ASC
            """;

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("prod_day", NormalizeDay(prodDay));
        return await ExecuteTableAsync(command, cancellationToken);
    }

    public async Task<DataTable> GetDemonHealthAsync(int limit = 500, CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT end_day, apply_machine, log, log_desc, status
            FROM k_demon_heath_check.total_demon_report
            ORDER BY end_day DESC, end_time DESC
            LIMIT @limit
            """;

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("limit", limit);
        return await ExecuteTableAsync(command, cancellationToken);
    }

    public async Task UpdateNonOperationReasonAsync(
        string prodDay,
        string shiftType,
        string station,
        string fromTs,
        string toTs,
        string reason,
        string sparepart,
        CancellationToken cancellationToken = default)
    {
        const string sql = """
            UPDATE i_daily_report.total_non_operation_time
               SET reason = @reason,
                   sparepart = @sparepart
             WHERE replace(prod_day::text, '-', '') = @prod_day
               AND lower(shift_type::text) = @shift_type
               AND station::text = @station
               AND to_char(from_ts AT TIME ZONE 'Asia/Seoul', 'HH24:MI:SS') = @from_ts
               AND to_char(to_ts AT TIME ZONE 'Asia/Seoul', 'HH24:MI:SS') = @to_ts
            """;

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("prod_day", NormalizeDay(prodDay));
        command.Parameters.AddWithValue("shift_type", NormalizeShift(shiftType));
        command.Parameters.AddWithValue("station", station);
        command.Parameters.AddWithValue("from_ts", NormalizeHhMmSs(fromTs));
        command.Parameters.AddWithValue("to_ts", NormalizeHhMmSs(toTs));
        command.Parameters.AddWithValue("reason", string.IsNullOrWhiteSpace(reason) ? DBNull.Value : reason.Trim());
        command.Parameters.AddWithValue("sparepart", string.IsNullOrWhiteSpace(sparepart) ? DBNull.Value : sparepart.Trim());

        var affected = await command.ExecuteNonQueryAsync(cancellationToken);
        if (affected == 0)
        {
            throw new InvalidOperationException("비가동 시간 대상 행을 찾지 못했습니다.");
        }
    }

    public async Task SaveEmailListAsync(DataTable original, DataTable edited, CancellationToken cancellationToken = default)
    {
        var oldSet = RowSet(original, "email");
        var newSet = RowSet(edited, "email");

        await using var connection = await OpenAsync(cancellationToken);
        var emailColumn = await GetEmailListColumnAsync(connection, cancellationToken);
        await using var tx = await connection.BeginTransactionAsync(cancellationToken);

        foreach (var email in oldSet.Except(newSet))
        {
            await ExecuteNonQueryAsync(connection, tx, $"DELETE FROM g_production_film.email_list WHERE lower(btrim({emailColumn})) = @email", cancellationToken, ("email", email));
        }

        foreach (var email in newSet.Except(oldSet))
        {
            await ExecuteNonQueryAsync(connection, tx, $"INSERT INTO g_production_film.email_list ({emailColumn}) VALUES (@email) ON CONFLICT DO NOTHING", cancellationToken, ("email", email));
        }

        await tx.CommitAsync(cancellationToken);
    }

    public async Task SaveRemarkInfoAsync(DataTable original, DataTable edited, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var tx = await connection.BeginTransactionAsync(cancellationToken);

        var oldKeys = RowSet(original, "barcode_information");
        var newRows = edited.Rows.Cast<DataRow>()
            .Select(r => new
            {
                Barcode = Clean(r["barcode_information"]),
                Pn = Clean(r["pn"]),
                Remark = Clean(r["remark"]),
            })
            .Where(r => r.Barcode.Length > 0)
            .GroupBy(r => r.Barcode)
            .Select(g => g.Last())
            .ToList();
        var newKeys = newRows.Select(r => r.Barcode).ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var barcode in oldKeys.Except(newKeys))
        {
            await ExecuteNonQueryAsync(connection, tx, "DELETE FROM g_production_film.remark_info WHERE lower(btrim(barcode_information)) = @barcode", cancellationToken, ("barcode", barcode));
        }

        const string upsert = """
            INSERT INTO g_production_film.remark_info (barcode_information, pn, remark)
            VALUES (@barcode, @pn, @remark)
            ON CONFLICT (barcode_information)
            DO UPDATE SET pn = EXCLUDED.pn, remark = EXCLUDED.remark
            """;

        foreach (var row in newRows)
        {
            await ExecuteNonQueryAsync(connection, tx, upsert, cancellationToken, ("barcode", row.Barcode), ("pn", row.Pn), ("remark", row.Remark));
        }

        await tx.CommitAsync(cancellationToken);
    }

    public async Task SavePlannedTimeAsync(DataTable original, DataTable edited, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var tx = await connection.BeginTransactionAsync(cancellationToken);

        var oldKeys = PlannedKeys(original);
        var newRows = edited.Rows.Cast<DataRow>()
            .Select(r => new
            {
                EndDay = NormalizeDay(Clean(r["end_day"])),
                From = NormalizeHhMmSs(Clean(r["from_time"])),
                To = NormalizeHhMmSs(Clean(r["to_time"])),
                Reason = Clean(r["reason"]),
            })
            .Where(r => r.From.Length > 0 && r.To.Length > 0)
            .ToList();
        var newKeys = newRows.Select(r => $"{r.EndDay}|{r.From}|{r.To}").ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var key in oldKeys.Except(newKeys))
        {
            var p = key.Split('|');
            await ExecuteNonQueryAsync(
                connection,
                tx,
                "DELETE FROM g_production_film.planned_time WHERE end_day = @end_day AND from_time = @from_time AND to_time = @to_time",
                cancellationToken,
                ("end_day", p[0]),
                ("from_time", p[1]),
                ("to_time", p[2]));
        }

        const string upsert = """
            INSERT INTO g_production_film.planned_time (end_day, from_time, to_time, reason)
            VALUES (@end_day, @from_time, @to_time, @reason)
            ON CONFLICT (end_day, from_time, to_time)
            DO UPDATE SET reason = EXCLUDED.reason
            """;

        foreach (var row in newRows)
        {
            await ExecuteNonQueryAsync(connection, tx, upsert, cancellationToken, ("end_day", row.EndDay), ("from_time", row.From), ("to_time", row.To), ("reason", row.Reason));
        }

        await tx.CommitAsync(cancellationToken);
    }

    public async Task SaveWorkerInfoAsync(DataTable original, DataTable edited, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var tx = await connection.BeginTransactionAsync(cancellationToken);

        var oldKeys = WorkerKeys(original);
        var newRows = edited.Rows.Cast<DataRow>()
            .Select(r => new
            {
                EndDay = NormalizeDay(Clean(r["end_day"])),
                Shift = UiShiftToDb(Clean(r["shift_type"])),
                Worker = Clean(r["worker_name"]),
                Order = Clean(r["order_number"]),
            })
            .Where(r => r.Worker.Length > 0)
            .ToList();
        var newKeys = newRows.Select(r => $"{r.EndDay}|{r.Shift}|{r.Worker.ToLowerInvariant()}").ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var key in oldKeys.Except(newKeys))
        {
            var p = key.Split('|');
            await ExecuteNonQueryAsync(
                connection,
                tx,
                "DELETE FROM g_production_film.worker_info WHERE end_day = @end_day AND shift_type = @shift_type AND lower(worker_name) = @worker_name",
                cancellationToken,
                ("end_day", p[0]),
                ("shift_type", p[1]),
                ("worker_name", p[2]));
        }

        const string upsert = """
            INSERT INTO g_production_film.worker_info (end_day, shift_type, worker_name, order_number, created_at, updated_at)
            VALUES (@end_day, @shift_type, @worker_name, @order_number, now(), now())
            ON CONFLICT (end_day, shift_type, worker_name)
            DO UPDATE SET order_number = EXCLUDED.order_number, updated_at = now()
            """;

        foreach (var row in newRows)
        {
            await ExecuteNonQueryAsync(connection, tx, upsert, cancellationToken, ("end_day", row.EndDay), ("shift_type", row.Shift), ("worker_name", row.Worker), ("order_number", row.Order));
        }

        await tx.CommitAsync(cancellationToken);
    }

    private async Task<NpgsqlConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static async Task<string> GetEmailListColumnAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'g_production_film'
              AND table_name = 'email_list'
              AND column_name IN ('email', 'email_list')
            ORDER BY CASE column_name WHEN 'email' THEN 0 ELSE 1 END
            LIMIT 1
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        var column = Convert.ToString(await command.ExecuteScalarAsync(cancellationToken));
        return column == "email" ? "email" : "email_list";
    }

    private async Task<DataTable> QueryByDayAsync(string schema, string table, string dayColumn, string prodDay, string shiftType, bool excludeUpdatedAt, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        var columns = await GetColumnsAsync(connection, schema, table, cancellationToken);
        if (columns.Count == 0)
        {
            return new DataTable();
        }

        var selectedColumns = columns
            .Where(c => !(excludeUpdatedAt && string.Equals(c, "updated_at", StringComparison.OrdinalIgnoreCase)))
            .ToList();
        var hasShift = columns.Contains("shift_type", StringComparer.OrdinalIgnoreCase);

        var where = $"replace(cast({QuoteIdentifier(dayColumn)} as text), '-', '') = @prod_day";
        if (hasShift)
        {
            where += " AND lower(shift_type::text) = @shift_type";
        }

        var sql = $"""
            SELECT {string.Join(", ", selectedColumns.Select(QuoteIdentifier))}
            FROM {QuoteIdentifier(schema)}.{QuoteIdentifier(table)}
            WHERE {where}
            {OrderBy(columns)}
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("prod_day", NormalizeDay(prodDay));
        if (hasShift)
        {
            command.Parameters.AddWithValue("shift_type", NormalizeShift(shiftType));
        }

        return await ExecuteTableAsync(command, cancellationToken);
    }

    private static async Task<List<string>> GetColumnsAsync(NpgsqlConnection connection, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = @schema
              AND table_name = @table
            ORDER BY ordinal_position
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("schema", schema);
        command.Parameters.AddWithValue("table", table);

        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private static async Task<DataTable> ExecuteTableAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var table = new DataTable();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        table.Load(reader);
        foreach (DataColumn column in table.Columns)
        {
            column.ReadOnly = false;
        }
        return table;
    }

    private static DataTable FilterPlannedTimeByShift(DataTable source, string prodDay, string shiftType)
    {
        var result = source.Clone();
        var day = DateTime.ParseExact(NormalizeDay(prodDay), "yyyyMMdd", null);
        var dayStart = day.AddHours(8).AddMinutes(30);
        var dayEnd = day.AddHours(20).AddMinutes(30);
        var nightStart = dayEnd;
        var nightEnd = day.AddDays(1).AddHours(8).AddMinutes(30);

        foreach (DataRow row in source.Rows)
        {
            var from = CombinePlannedDateTime(day, Clean(row["from_time"]));
            var to = CombinePlannedDateTime(day, Clean(row["to_time"]));
            if (to <= from)
            {
                to = to.AddDays(1);
            }

            var dayMinutes = OverlapMinutes(from, to, dayStart, dayEnd);
            var nightMinutes = OverlapMinutes(from, to, nightStart, nightEnd);
            var assigned = nightMinutes > dayMinutes ? "night" : "day";
            if (assigned == shiftType)
            {
                result.ImportRow(row);
            }
        }

        return result;
    }

    private static void NormalizePlannedTimeDisplay(DataTable table)
    {
        foreach (DataColumn column in table.Columns)
        {
            column.ReadOnly = false;
        }

        foreach (DataRow row in table.Rows)
        {
            if (table.Columns.Contains("end_day"))
            {
                var day = NormalizeDay(Clean(row["end_day"]));
                row["end_day"] = $"{day[..4]}-{day[4..6]}-{day[6..8]}";
            }
            if (table.Columns.Contains("from_time"))
            {
                row["from_time"] = NormalizeHhMmSs(Clean(row["from_time"]));
            }
            if (table.Columns.Contains("to_time"))
            {
                row["to_time"] = NormalizeHhMmSs(Clean(row["to_time"]));
            }
        }
    }

    private static DateTime CombinePlannedDateTime(DateTime day, string time)
    {
        if (!TimeSpan.TryParse(NormalizeHhMmSs(time), out var span))
        {
            return day;
        }
        return day.Date.Add(span);
    }

    private static double OverlapMinutes(DateTime from, DateTime to, DateTime windowFrom, DateTime windowTo)
    {
        var start = from > windowFrom ? from : windowFrom;
        var end = to < windowTo ? to : windowTo;
        return end > start ? (end - start).TotalMinutes : 0;
    }

    private static void MakeEditable(DataTable table, params string[] columns)
    {
        foreach (var name in columns)
        {
            if (table.Columns.Contains(name))
            {
                table.Columns[name]!.ReadOnly = false;
            }
        }
    }

    private static async Task ExecuteNonQueryAsync(NpgsqlConnection connection, NpgsqlTransaction tx, string sql, CancellationToken cancellationToken, params (string Name, object? Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection, tx);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string OrderBy(IReadOnlyCollection<string> columns)
    {
        if (columns.Contains("end_time", StringComparer.OrdinalIgnoreCase))
        {
            return "ORDER BY \"end_time\" DESC";
        }
        if (columns.Contains("from_ts", StringComparer.OrdinalIgnoreCase))
        {
            return "ORDER BY \"from_ts\" DESC";
        }
        if (columns.Contains("station", StringComparer.OrdinalIgnoreCase))
        {
            return "ORDER BY \"station\" ASC";
        }
        if (columns.Contains("line", StringComparer.OrdinalIgnoreCase))
        {
            return "ORDER BY \"line\" ASC";
        }
        return "";
    }

    private static string NormalizeConnectionString(string value)
    {
        var raw = value.Trim();
        if (string.IsNullOrWhiteSpace(raw))
        {
            throw new InvalidOperationException("DATABASE_URL is empty. Check app\\.env.");
        }

        raw = raw.Replace("postgresql+psycopg2://", "postgresql://", StringComparison.OrdinalIgnoreCase);
        raw = raw.Replace("postgres+psycopg2://", "postgresql://", StringComparison.OrdinalIgnoreCase);

        if (!raw.Contains("://", StringComparison.Ordinal))
        {
            return raw;
        }

        var uri = new Uri(raw);
        var userInfo = uri.UserInfo.Split(':', 2);
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = uri.Host,
            Port = uri.Port > 0 ? uri.Port : 5432,
            Database = uri.AbsolutePath.TrimStart('/'),
            Username = Uri.UnescapeDataString(userInfo.ElementAtOrDefault(0) ?? ""),
            Password = Uri.UnescapeDataString(userInfo.ElementAtOrDefault(1) ?? ""),
            Timeout = 10,
            CommandTimeout = 60,
            Pooling = true,
        };

        return builder.ConnectionString;
    }

    private static string NormalizeDay(string value)
    {
        var digits = new string((value ?? "").Where(char.IsDigit).ToArray());
        if (digits.Length < 8)
        {
            throw new InvalidOperationException("day must be YYYYMMDD or YYYY-MM-DD.");
        }
        return digits[..8];
    }

    private static string NormalizeShift(string value)
    {
        var shift = (value ?? "").Trim().ToLowerInvariant();
        return shift switch
        {
            "day" or "주간" => "day",
            "night" or "야간" => "night",
            _ => throw new InvalidOperationException("shift_type must be day/night."),
        };
    }

    private static string UiShiftToDb(string value) => NormalizeShift(value);

    private static string NormalizeHhMmSs(string value)
    {
        var s = (value ?? "").Trim();
        if (s.Length == 5 && s[2] == ':')
        {
            return s + ":00";
        }
        if (s.Length >= 8 && s[2] == ':' && s[5] == ':')
        {
            return s[..8];
        }
        return s;
    }

    private static string QuoteIdentifier(string value) => "\"" + value.Replace("\"", "\"\"") + "\"";

    private static string Clean(object? value) => Convert.ToString(value)?.Trim() ?? "";

    private static HashSet<string> RowSet(DataTable table, string column)
    {
        return table.Rows.Cast<DataRow>()
            .Select(r => Clean(r[column]).ToLowerInvariant())
            .Where(v => v.Length > 0)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private static HashSet<string> PlannedKeys(DataTable table)
    {
        return table.Rows.Cast<DataRow>()
            .Select(r => $"{NormalizeDay(Clean(r["end_day"]))}|{NormalizeHhMmSs(Clean(r["from_time"]))}|{NormalizeHhMmSs(Clean(r["to_time"]))}")
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private static HashSet<string> WorkerKeys(DataTable table)
    {
        return table.Rows.Cast<DataRow>()
            .Select(r => $"{NormalizeDay(Clean(r["end_day"]))}|{UiShiftToDb(Clean(r["shift_type"]))}|{Clean(r["worker_name"]).ToLowerInvariant()}")
            .Where(k => !k.EndsWith("|", StringComparison.Ordinal))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }
}
