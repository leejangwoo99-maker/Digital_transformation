using System.Data;
using System.Net;
using System.Net.Mail;
using System.Globalization;
using System.Text.RegularExpressions;
using PdfSharp.Drawing;
using PdfSharp.Fonts;
using PdfSharp.Pdf;

namespace AptivDashboard.Gui;

internal sealed class DashboardReportSender
{
    private readonly Func<DatabaseClient> _dbFactory;

    public DashboardReportSender(Func<DatabaseClient> dbFactory)
    {
        _dbFactory = dbFactory;
    }

    public async Task<string> SendAsync(DashboardScope scope, string trigger, CancellationToken cancellationToken = default)
    {
        var db = _dbFactory();
        var recipients = await GetRecipientsAsync(db, cancellationToken);
        if (recipients.Count == 0)
        {
            throw new InvalidOperationException("Email list에 저장된 주소가 없습니다.");
        }

        var report = await BuildReportDataAsync(db, scope, cancellationToken);
        var pdfPath = CreatePdf(report, scope, trigger);
        try
        {
            await SendMailAsync(recipients, pdfPath, scope, trigger, cancellationToken);
            return "메일 첨부 전송 완료";
        }
        finally
        {
            TryDelete(pdfPath);
        }
    }

    private static async Task<List<string>> GetRecipientsAsync(DatabaseClient db, CancellationToken cancellationToken)
    {
        var table = await db.GetEmailListAsync(cancellationToken);
        if (!table.Columns.Contains("email"))
        {
            return [];
        }

        return table.Rows.Cast<DataRow>()
            .Select(row => Convert.ToString(row["email"])?.Trim() ?? "")
            .Where(email => email.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static async Task<List<ReportSection>> BuildReportDataAsync(DatabaseClient db, DashboardScope scope, CancellationToken cancellationToken)
    {
        var nonOperation = await db.GetNonOperationAsync(scope, reasonOnly: false, cancellationToken);
        var planned = await db.GetPlannedTimeAsync(scope.ProdDay, cancellationToken: cancellationToken);
        var pdBoard = await db.GetPdBoardAsync(scope.ProdDay, cancellationToken);

        var sections = new List<ReportSection>
        {
            new("실시간 생산 진행 현황", nonOperation, "status", planned, AlwaysShowTitle: true),
            new("작업자 정보", await db.GetWorkerInfoAsync(scope, cancellationToken), AlwaysShowTitle: true),
            new("Master Sample", await db.GetReportAsync("mastersample", scope, cancellationToken)),
            new("비가동 전체 (Minor제외)", await db.GetNonOperationAsync(scope, reasonOnly: true, cancellationToken), AlwaysShowTitle: true),
            new("OEE Total", await db.GetReportAsync("oee_total", scope, cancellationToken)),
            new("OEE Line", await db.GetReportAsync("oee_line", scope, cancellationToken)),
            new("OEE Station", await db.GetReportAsync("oee_station", scope, cancellationToken)),
            new("계획 정지 시간", await db.GetReportAsync("planned_stop", scope, cancellationToken)),
            new("비가동 시간", await db.GetReportAsync("non_time", scope, cancellationToken)),
            new("품번별 총 생산량", await db.GetReportAsync("final_amount", scope, cancellationToken)),
            new("TEST 합격률", await db.GetReportAsync("pass_percent", scope, cancellationToken)),
            new("FAIL LIST 1회", await db.GetReportAsync("fct_fail_1", scope, cancellationToken)),
            new("FAIL LIST 2회", await db.GetReportAsync("fct_fail_2", scope, cancellationToken)),
            new("FAIL LIST 3회", await db.GetReportAsync("fct_fail_3", scope, cancellationToken)),
            new("Sparepart 알람", await db.GetRecentAlarmsAsync(scope.ProdDay, cancellationToken: cancellationToken)),
            new("PD-Board 열화 모니터링(WARNING은 교체 검토 필요 및 모니터링 필요 / CRITICAL은 교체 필요)", SelectColumns(pdBoard, "end_day", "station", "last_status"), "pd", pdBoard, AlwaysShowTitle: true),
            new("FCT Worst Case", await db.GetReportAsync("worst_case", scope, cancellationToken)),
            new("조립 불량 손실", await db.GetReportAsync("afa_wasted", scope, cancellationToken)),
            new("MES 불량 손실", await db.GetReportAsync("mes_wasted", scope, cancellationToken)),
            new("프로그램 상태", await db.GetDemonHealthAsync(cancellationToken: cancellationToken)),
        };

        return sections;
    }

    private static string CreatePdf(List<ReportSection> sections, DashboardScope scope, string trigger)
    {
        EnsurePdfFonts();
        var dir = Path.Combine(Path.GetTempPath(), "AptivDashboardReports");
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, $"APTIV_{scope.ProdDay}_{scope.ShiftType}_{DateTime.Now:yyyyMMdd_HHmmss}.pdf");

        var document = new PdfDocument();
        document.Info.Title = $"APTIV Dashboard {scope.ProdDay} {scope.ShiftType}";

        var font = new XFont("Malgun Gothic", 8, XFontStyleEx.Regular);
        var titleFont = new XFont("Malgun Gothic", 15, XFontStyleEx.Bold);
        var sectionFont = new XFont("Malgun Gothic", 11, XFontStyleEx.Bold);
        var headerFont = new XFont("Malgun Gothic", 8, XFontStyleEx.Bold);

        var page = document.AddPage();
        page.Size = PdfSharp.PageSize.A3;
        page.Orientation = PdfSharp.PageOrientation.Landscape;
        var gfx = XGraphics.FromPdfPage(page);
        var y = 34.0;
        DrawTitle(gfx, page, titleFont, scope, trigger, ref y);

        foreach (var section in sections)
        {
            DrawSection(document, ref page, ref gfx, section, scope, font, headerFont, sectionFont, ref y);
        }

        document.Save(path);
        return path;
    }

    private static DataTable SelectColumns(DataTable source, params string[] columns)
    {
        var table = new DataTable();
        foreach (var column in columns)
        {
            if (source.Columns.Contains(column))
            {
                table.Columns.Add(column, typeof(string));
            }
        }

        foreach (DataRow sourceRow in source.Rows)
        {
            var row = table.NewRow();
            foreach (DataColumn column in table.Columns)
            {
                row[column.ColumnName] = Convert.ToString(sourceRow[column.ColumnName]) ?? "";
            }
            table.Rows.Add(row);
        }
        return table;
    }

    private static string Value(DataRow row, string column) => row.Table.Columns.Contains(column)
        ? Convert.ToString(row[column]) ?? ""
        : "";

    private static void EnsurePdfFonts()
    {
        if (GlobalFontSettings.FontResolver is null)
        {
            GlobalFontSettings.FontResolver = new MalgunFontResolver();
        }
    }

    private static void DrawTitle(XGraphics gfx, PdfPage page, XFont titleFont, DashboardScope scope, string trigger, ref double y)
    {
        gfx.DrawString("APTIV Dashboard Report", titleFont, XBrushes.Black, new XPoint(28, y));
        y += 22;
        gfx.DrawString($"생산일자: {scope.ProdDay}   Shift: {scope.ShiftType}   발송: {trigger}   생성: {DateTime.Now:yyyy-MM-dd HH:mm:ss}",
            new XFont("Malgun Gothic", 9, XFontStyleEx.Regular),
            XBrushes.Black,
            new XPoint(28, y));
        y += 24;
        gfx.DrawLine(XPens.Gray, 28, y, page.Width.Point - 28, y);
        y += 14;
    }

    private static void DrawSection(
        PdfDocument document,
        ref PdfPage page,
        ref XGraphics gfx,
        ReportSection section,
        DashboardScope scope,
        XFont font,
        XFont headerFont,
        XFont sectionFont,
        ref double y)
    {
        if (section.Table.Rows.Count == 0 && !section.AlwaysShowTitle && section.ChartKind is null)
        {
            return;
        }

        EnsureSpace(document, ref page, ref gfx, ref y, 70);
        gfx.DrawString(section.Title, sectionFont, XBrushes.Black, new XPoint(28, y));
        y += 16;

        if (section.ChartKind == "status")
        {
            DrawStatusChart(document, ref page, ref gfx, section.Table, section.ExtraTable, scope, ref y);
            return;
        }

        if (section.ChartKind == "pd")
        {
            DrawTable(document, ref page, ref gfx, section, font, headerFont, ref y);
            DrawPdChart(document, ref page, ref gfx, section.ExtraTable ?? section.Table, ref y);
            return;
        }

        DrawTable(document, ref page, ref gfx, section, font, headerFont, ref y);
    }

    private static void DrawTable(
        PdfDocument document,
        ref PdfPage page,
        ref XGraphics gfx,
        ReportSection section,
        XFont font,
        XFont headerFont,
        ref double y)
    {
        var visibleColumns = section.Table.Columns.Cast<DataColumn>()
            .Where(column => section.Table.Rows.Cast<DataRow>().Any(row => !string.IsNullOrWhiteSpace(Convert.ToString(row[column]))))
            .ToList();
        if (visibleColumns.Count == 0)
        {
            y += 10;
            return;
        }

        var width = page.Width.Point - 56;
        var weights = visibleColumns.Select(ColumnWeight).ToList();
        var totalWeight = weights.Sum();
        var columnWidths = weights.Select(weight => width * weight / totalWeight).ToList();
        var rowFont = visibleColumns.Count > 16 ? new XFont("Malgun Gothic", 5.2, XFontStyleEx.Regular) : visibleColumns.Count > 12 ? new XFont("Malgun Gothic", 6.2, XFontStyleEx.Regular) : font;
        var headFont = visibleColumns.Count > 16 ? new XFont("Malgun Gothic", 5.4, XFontStyleEx.Bold) : visibleColumns.Count > 12 ? new XFont("Malgun Gothic", 6.4, XFontStyleEx.Bold) : headerFont;
        if (section.Title == "TEST 합격률")
        {
            rowFont = new XFont("Malgun Gothic", Math.Max(4.8, rowFont.Size - 0.3), XFontStyleEx.Regular);
            headFont = new XFont("Malgun Gothic", Math.Max(5.0, headFont.Size - 0.3), XFontStyleEx.Bold);
        }

        EnsureSpace(document, ref page, ref gfx, ref y, 48);
        var left = 28.0;
        for (var i = 0; i < visibleColumns.Count; i++)
        {
            gfx.DrawString(FitText(visibleColumns[i].ColumnName, columnWidths[i], headFont), headFont, XBrushes.DarkBlue, new XRect(left, y, columnWidths[i], 14), XStringFormats.TopLeft);
            left += columnWidths[i];
        }
        y += 14;
        gfx.DrawLine(XPens.LightGray, 28, y, page.Width.Point - 28, y);
        y += 4;

        foreach (DataRow row in section.Table.Rows.Cast<DataRow>().Take(45))
        {
            EnsureSpace(document, ref page, ref gfx, ref y, 18);
            left = 28;
            for (var i = 0; i < visibleColumns.Count; i++)
            {
                var text = Convert.ToString(row[visibleColumns[i]]) ?? "";
                gfx.DrawString(FitText(text, columnWidths[i], rowFont), rowFont, XBrushes.Black, new XRect(left, y, columnWidths[i], 12), XStringFormats.TopLeft);
                left += columnWidths[i];
            }
            y += rowFont.Size + 5;
        }
        y += 12;
    }

    private static void EnsureSpace(PdfDocument document, ref PdfPage page, ref XGraphics gfx, ref double y, double required)
    {
        if (y + required < page.Height.Point - 28)
        {
            return;
        }

        page = document.AddPage();
        page.Size = PdfSharp.PageSize.A3;
        page.Orientation = PdfSharp.PageOrientation.Landscape;
        gfx = XGraphics.FromPdfPage(page);
        y = 34;
    }

    private static void DrawStatusChart(PdfDocument document, ref PdfPage page, ref XGraphics gfx, DataTable nonOperation, DataTable? planned, DashboardScope scope, ref double y)
    {
        EnsureSpace(document, ref page, ref gfx, ref y, 270);
        var (start, end) = GetWindow(scope);
        var now = DateTime.Now;
        var elapsedEnd = now <= start ? start : now >= end ? end : now;
        var totalMinutes = Math.Max(1, (end - start).TotalMinutes);
        var plotWidth = Math.Min(620, page.Width.Point - 120);
        var plotHeight = 220.0;
        var plotLeft = (page.Width.Point - plotWidth) / 2;
        var plot = new XRect(plotLeft, y + 38, plotWidth, plotHeight);
        var stations = new[] { "FCT1", "FCT2", "FCT3", "FCT4", "Vision1", "Vision2" };
        var run = new XSolidBrush(XColor.FromArgb(136, 211, 182));
        var plan = new XSolidBrush(XColor.FromArgb(246, 231, 167));
        var stop = new XSolidBrush(XColor.FromArgb(244, 180, 180));
        var future = new XSolidBrush(XColor.FromArgb(220, 220, 220));
        var font = new XFont("Malgun Gothic", 7, XFontStyleEx.Regular);

        DrawLegend(gfx, plot.Right - 250, y + 10, [("가동", run), ("계획 정지", plan), ("비가동", stop), ("미작업시간", future)]);
        gfx.DrawRectangle(XPens.Gray, plot);

        for (var t = start; t <= end.AddSeconds(1); t = t.AddHours(1))
        {
            var yy = TimeToY(t, start, totalMinutes, plot);
            gfx.DrawLine(XPens.LightGray, plot.Left, yy, plot.Right, yy);
            gfx.DrawString(t.ToString("HH:mm"), font, XBrushes.Black, new XPoint(plot.Left - 42, yy + 3));
        }

        var gap = Math.Max(8, plot.Width / 100);
        var barWidth = Math.Max(34, (plot.Width - gap * (stations.Length + 1)) / stations.Length);
        for (var i = 0; i < stations.Length; i++)
        {
            var station = stations[i];
            var x = plot.Left + gap + i * (barWidth + gap);
            var bar = new XRect(x, plot.Top, barWidth, plot.Height);
            FillSegment(gfx, run, bar, start, elapsedEnd, start, totalMinutes, plot);
            FillSegment(gfx, future, bar, elapsedEnd, end, start, totalMinutes, plot);
            DrawStopSegments(gfx, stop, bar, station, nonOperation, start, end, totalMinutes, plot);
            DrawPlannedSegments(gfx, plan, bar, planned, start, end, totalMinutes, plot);
            gfx.DrawRectangle(XPens.White, bar);
            gfx.DrawString(station, font, XBrushes.Black, new XRect(x, plot.Top - 15, barWidth, 12), XStringFormats.TopCenter);
        }

        y = plot.Bottom + 20;
    }

    private static void DrawPdChart(PdfDocument document, ref PdfPage page, ref XGraphics gfx, DataTable table, ref double y)
    {
        var series = ParsePdSeries(table);
        if (series.Count == 0)
        {
            return;
        }

        EnsureSpace(document, ref page, ref gfx, ref y, 270);
        var titleFont = new XFont("Malgun Gothic", 9, XFontStyleEx.Regular);
        var font = new XFont("Malgun Gothic", 7, XFontStyleEx.Regular);
        gfx.DrawString("Cosine Similarity to Abnormal Reference", titleFont, XBrushes.Black, new XPoint(28, y + 12));

        var plot = new XRect(58, y + 55, page.Width.Point - 86, 210);
        gfx.DrawRectangle(XPens.LightGray, plot);
        var allPoints = series.SelectMany(s => s.Points).ToList();
        var labels = allPoints.Select(p => p.Label).Distinct().ToList();
        var minY = Math.Min(-0.6, allPoints.Min(p => p.Value));
        var threshold = series.Select(s => s.Threshold).FirstOrDefault(v => v is not null) ?? 0.7;
        var maxY = Math.Max(0.8, Math.Max(allPoints.Max(p => p.Value), threshold));

        for (var i = 0; i <= 5; i++)
        {
            var value = minY + (maxY - minY) * i / 5;
            var yy = ValueToY(value, minY, maxY, plot);
            gfx.DrawLine(XPens.LightGray, plot.Left, yy, plot.Right, yy);
            gfx.DrawString(value.ToString("0.0", CultureInfo.InvariantCulture), font, XBrushes.Black, new XPoint(20, yy + 3));
        }

        for (var i = 0; i < labels.Count; i++)
        {
            var x = IndexToX(i, labels.Count, plot);
            gfx.DrawLine(XPens.LightGray, x, plot.Top, x, plot.Bottom);
            gfx.DrawString(ShortLabel(labels[i]), font, XBrushes.Black, new XRect(x - 20, plot.Bottom + 4, 40, 12), XStringFormats.TopCenter);
        }

        var colors = new[] { XColors.Blue, XColors.DarkOrange, XColors.Green, XColors.Red, XColors.Purple };
        var legendX = 28.0;
        for (var i = 0; i < series.Count; i++)
        {
            var pen = new XPen(colors[i % colors.Length], 1.3);
            gfx.DrawLine(pen, legendX, y + 34, legendX + 20, y + 34);
            gfx.DrawString(series[i].Name, font, XBrushes.Black, new XPoint(legendX + 24, y + 37));
            DrawPdSeries(gfx, series[i], labels, minY, maxY, plot, pen);
            legendX += 55;
        }
        var thPen = new XPen(XColor.FromArgb(92, 150, 185), 1.2) { DashStyle = XDashStyle.Dash };
        var thY = ValueToY(threshold, minY, maxY, plot);
        gfx.DrawLine(thPen, plot.Left, thY, plot.Right, thY);
        gfx.DrawLine(thPen, legendX, y + 34, legendX + 20, y + 34);
        gfx.DrawString("cos_th", font, XBrushes.Black, new XPoint(legendX + 24, y + 37));

        y = plot.Bottom + 28;
    }

    private static double ColumnWeight(DataColumn column)
    {
        var name = column.ColumnName.ToLowerInvariant();
        if (name is "prod_day" or "shift_type" or "end_day")
        {
            return name == "shift_type" ? 0.36 : 0.42;
        }
        if (name is "station" or "line" or "pn" or "remark")
        {
            return 0.62;
        }
        if (name.Contains("시간대") || name.Contains("fct") || name.Contains("vision") || name.Contains("pass"))
        {
            return 1.25;
        }
        return 1.0;
    }

    private static void DrawLegend(XGraphics gfx, double x, double y, (string Label, XBrush Brush)[] items)
    {
        var font = new XFont("Malgun Gothic", 7, XFontStyleEx.Regular);
        foreach (var (label, brush) in items)
        {
            gfx.DrawRectangle(brush, x, y + 2, 10, 8);
            gfx.DrawString(label, font, XBrushes.Black, new XPoint(x + 14, y + 10));
            x += label.Length * 8 + 36;
        }
    }

    private static void DrawPlannedSegments(XGraphics gfx, XBrush brush, XRect bar, DataTable? planned, DateTime start, DateTime end, double totalMinutes, XRect plot)
    {
        if (planned is null || !planned.Columns.Contains("from_time") || !planned.Columns.Contains("to_time"))
        {
            return;
        }

        foreach (DataRow row in planned.Rows)
        {
            var from = CombineTime(start, Convert.ToString(row["from_time"]));
            var to = CombineTime(start, Convert.ToString(row["to_time"]));
            if (to < from)
            {
                to = to.AddDays(1);
            }
            FillSegment(gfx, brush, bar, from, to, start, totalMinutes, plot, end);
        }
    }

    private static void DrawStopSegments(XGraphics gfx, XBrush brush, XRect bar, string station, DataTable nonOperation, DateTime start, DateTime end, double totalMinutes, XRect plot)
    {
        foreach (DataRow row in nonOperation.Rows)
        {
            if (!string.Equals(Convert.ToString(row["station"]), station, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }
            if (TryParseDateTime(row["from_ts"], start, out var from) && TryParseDateTime(row["to_ts"], start, out var to))
            {
                if (to < from)
                {
                    to = to.AddDays(1);
                }
                FillSegment(gfx, brush, bar, from, to, start, totalMinutes, plot, end);
            }
        }
    }

    private static void FillSegment(XGraphics gfx, XBrush brush, XRect bar, DateTime from, DateTime to, DateTime start, double totalMinutes, XRect plot, DateTime? endLimit = null)
    {
        var end = endLimit ?? start.AddMinutes(totalMinutes);
        var a = from < start ? start : from;
        var b = to > end ? end : to;
        if (b <= a)
        {
            return;
        }

        var y1 = TimeToY(a, start, totalMinutes, plot);
        var y2 = TimeToY(b, start, totalMinutes, plot);
        gfx.DrawRectangle(brush, bar.Left, y1, bar.Width, Math.Max(1, y2 - y1));
    }

    private static double TimeToY(DateTime time, DateTime start, double totalMinutes, XRect plot)
    {
        var ratio = Math.Clamp((time - start).TotalMinutes / totalMinutes, 0, 1);
        return plot.Top + plot.Height * ratio;
    }

    private static (DateTime Start, DateTime End) GetWindow(DashboardScope scope)
    {
        var d = DateTime.ParseExact(scope.ProdDay, "yyyyMMdd", null);
        if (scope.ShiftType == "day")
        {
            var start = d.AddHours(8).AddMinutes(30);
            return (start, start.AddHours(12));
        }

        var nightStart = d.AddHours(20).AddMinutes(30);
        return (nightStart, nightStart.AddHours(12));
    }

    private static DateTime CombineTime(DateTime windowStart, string? timeText)
    {
        if (!TimeSpan.TryParse((timeText ?? "").Trim(), out var t))
        {
            return windowStart;
        }

        var dt = windowStart.Date.Add(t);
        if (windowStart.Hour >= 20 && t.Hours < 12)
        {
            dt = dt.AddDays(1);
        }
        return dt;
    }

    private static bool TryParseDateTime(object? value, DateTime windowStart, out DateTime dt)
    {
        if (DateTimeOffset.TryParse(Convert.ToString(value), out var dto))
        {
            dt = dto.LocalDateTime;
            return true;
        }
        if (DateTime.TryParse(Convert.ToString(value), out dt))
        {
            return true;
        }
        if (TimeSpan.TryParse(Convert.ToString(value), out var t))
        {
            dt = windowStart.Date.Add(t);
            if (windowStart.Hour >= 20 && t.Hours < 12)
            {
                dt = dt.AddDays(1);
            }
            return true;
        }
        return false;
    }

    private static List<PdSeries> ParsePdSeries(DataTable table)
    {
        if (!table.Columns.Contains("station") || !table.Columns.Contains("cosine_similarity"))
        {
            return [];
        }

        return table.Rows.Cast<DataRow>()
            .Select(row => ParseCosineData(Convert.ToString(row["station"]) ?? "", Convert.ToString(row["cosine_similarity"]) ?? ""))
            .Where(series => series.Points.Count > 0)
            .ToList();
    }

    private static PdSeries ParseCosineData(string station, string raw)
    {
        var labels = ExtractArray(raw, "x");
        var values = ExtractArray(raw, "y")
            .Select(v => double.TryParse(v, NumberStyles.Float, CultureInfo.InvariantCulture, out var n) ? n : double.NaN)
            .Where(v => !double.IsNaN(v))
            .ToList();
        var threshold = ExtractNumber(raw, "th");
        var count = Math.Min(labels.Count, values.Count);
        var points = new List<PdPoint>();
        for (var i = 0; i < count; i++)
        {
            points.Add(new PdPoint(labels[i], values[i]));
        }
        return new PdSeries(station, points, threshold);
    }

    private static List<string> ExtractArray(string raw, string key)
    {
        var match = Regex.Match(raw, $@"['""]{Regex.Escape(key)}['""]\s*:\s*\[(?<items>[^\]]*)\]");
        return match.Success
            ? match.Groups["items"].Value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(v => v.Trim().Trim('\'', '"')).Where(v => v.Length > 0).ToList()
            : [];
    }

    private static double? ExtractNumber(string raw, string key)
    {
        var match = Regex.Match(raw, $@"['""]{Regex.Escape(key)}['""]\s*:\s*(?<value>-?\d+(?:\.\d+)?)");
        return match.Success && double.TryParse(match.Groups["value"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var n) ? n : null;
    }

    private static void DrawPdSeries(XGraphics gfx, PdSeries series, List<string> labels, double minY, double maxY, XRect plot, XPen pen)
    {
        var points = series.Points.Select(p => new XPoint(IndexToX(labels.IndexOf(p.Label), labels.Count, plot), ValueToY(p.Value, minY, maxY, plot))).ToArray();
        if (points.Length > 1)
        {
            gfx.DrawLines(pen, points);
        }
        foreach (var point in points)
        {
            gfx.DrawEllipse(new XSolidBrush(pen.Color), point.X - 2, point.Y - 2, 4, 4);
        }
    }

    private static string ShortLabel(string label) => label.Length == 8 && label.All(char.IsDigit) ? $"{label[4..6]}-{label[6..8]}" : label;

    private static double IndexToX(int index, int count, XRect plot) => count <= 1 ? plot.Left + plot.Width / 2 : plot.Left + plot.Width * index / (count - 1);

    private static double ValueToY(double value, double minY, double maxY, XRect plot) => plot.Bottom - plot.Height * ((value - minY) / (maxY - minY));

    private static string FitText(string value, double width, XFont font)
    {
        var singleLine = value.ReplaceLineEndings(" ").Trim();
        var max = Math.Max(3, (int)(width / Math.Max(2.2, font.Size * 0.48)));
        return singleLine.Length <= max ? singleLine : singleLine[..Math.Max(0, max - 1)] + "…";
    }

    private static string TrimText(string value, int length)
    {
        var singleLine = value.ReplaceLineEndings(" ").Trim();
        return singleLine.Length <= length ? singleLine : singleLine[..Math.Max(0, length - 1)] + "...";
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Attachment temp files are best-effort cleanup only.
        }
    }

    private static async Task SendMailAsync(List<string> recipients, string pdfPath, DashboardScope scope, string trigger, CancellationToken cancellationToken)
    {
        var host = EnvFile.Get("SMTP_HOST");
        if (string.IsNullOrWhiteSpace(host))
        {
            throw new InvalidOperationException("SMTP_HOST가 설정되어 있지 않습니다. .env에 SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD 또는 SMTP_PASS, SMTP_FROM 값을 설정하세요.");
        }

        var port = int.TryParse(EnvFile.Get("SMTP_PORT", "587"), out var parsedPort) ? parsedPort : 587;
        var user = EnvFile.Get("SMTP_USER");
        var password = EnvFile.Get("SMTP_PASSWORD", EnvFile.Get("SMTP_PASS"));
        var from = EnvFile.Get("SMTP_FROM", user);
        var enableSsl = !string.Equals(EnvFile.Get("SMTP_ENABLE_SSL", "true"), "false", StringComparison.OrdinalIgnoreCase);

        using var message = new MailMessage
        {
            From = new MailAddress(from),
            Subject = $"[APTIV] Dashboard Report {scope.ProdDay} {scope.ShiftType}",
            Body = $"APTIV Dashboard 보고서입니다.\n\n생산일자: {scope.ProdDay}\nShift: {scope.ShiftType}\n발송 구분: {trigger}\n생성 시각: {DateTime.Now:yyyy-MM-dd HH:mm:ss}",
        };
        foreach (var recipient in recipients)
        {
            message.To.Add(recipient);
        }
        message.Attachments.Add(new Attachment(pdfPath));

        using var client = new SmtpClient(host, port)
        {
            EnableSsl = enableSsl,
        };
        if (!string.IsNullOrWhiteSpace(user))
        {
            client.Credentials = new NetworkCredential(user, password);
        }

        await client.SendMailAsync(message, cancellationToken);
    }

    private sealed record ReportSection(
        string Title,
        DataTable Table,
        string? ChartKind = null,
        DataTable? ExtraTable = null,
        bool AlwaysShowTitle = false);

    private sealed record PdSeries(string Name, List<PdPoint> Points, double? Threshold);

    private sealed record PdPoint(string Label, double Value);

    private sealed class MalgunFontResolver : IFontResolver
    {
        private const string Regular = "malgun";
        private const string Bold = "malgunbd";

        public FontResolverInfo ResolveTypeface(string familyName, bool isBold, bool isItalic)
        {
            return new FontResolverInfo(isBold ? Bold : Regular, mustSimulateBold: false, mustSimulateItalic: isItalic);
        }

        public byte[] GetFont(string faceName)
        {
            var fileName = faceName == Bold ? "malgunbd.ttf" : "malgun.ttf";
            var path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Fonts), fileName);
            if (!File.Exists(path))
            {
                path = Path.Combine(@"C:\Windows\Fonts", fileName);
            }
            return File.ReadAllBytes(path);
        }
    }
}
