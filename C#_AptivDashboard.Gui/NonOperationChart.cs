using System.Data;

namespace AptivDashboard.Gui;

internal sealed class NonOperationChart : Control
{
    private readonly string[] _stations = ["FCT1", "FCT2", "Vision1", "FCT3", "FCT4", "Vision2"];
    private DataTable? _nonOperation;
    private DataTable? _planned;
    private DashboardScope? _scope;

    public NonOperationChart()
    {
        DoubleBuffered = true;
        BackColor = Color.White;
    }

    public void SetData(DataTable? nonOperation, DataTable? planned, DashboardScope scope)
    {
        _nonOperation = nonOperation;
        _planned = planned;
        _scope = scope;
        Invalidate();
    }

    protected override void OnPaint(PaintEventArgs e)
    {
        base.OnPaint(e);
        var g = e.Graphics;
        g.SmoothingMode = System.Drawing.Drawing2D.SmoothingMode.AntiAlias;

        if (_scope is null || ClientSize.Width < 240 || ClientSize.Height < 180)
        {
            return;
        }

        var (start, end) = GetWindow(_scope);
        var now = DateTime.Now;
        var elapsedEnd = now <= start ? start : now >= end ? end : now;
        var totalMinutes = Math.Max(1, (end - start).TotalMinutes);

        using var titleFont = new Font(Font.FontFamily, 12, FontStyle.Bold);
        using var smallFont = new Font(Font.FontFamily, 8);
        using var textBrush = new SolidBrush(Color.FromArgb(28, 28, 28));
        using var gridPen = new Pen(Color.FromArgb(225, 225, 225));
        using var axisPen = new Pen(Color.FromArgb(110, 110, 110));
        using var runBrush = new SolidBrush(Color.FromArgb(136, 211, 182));
        using var planBrush = new SolidBrush(Color.FromArgb(246, 231, 167));
        using var stopBrush = new SolidBrush(Color.FromArgb(244, 180, 180));
        using var futureBrush = new SolidBrush(Color.FromArgb(220, 220, 220));

        g.DrawString("실시간 생산 진행 현황", titleFont, textBrush, 12, 8);
        DrawLegend(g, smallFont, textBrush, runBrush, planBrush, stopBrush, futureBrush);

        var plot = new Rectangle(58, 52, ClientSize.Width - 76, ClientSize.Height - 92);
        g.DrawRectangle(axisPen, plot);

        for (var t = start; t <= end.AddSeconds(1); t = t.AddHours(1))
        {
            var y = TimeToY(t, start, totalMinutes, plot);
            g.DrawLine(gridPen, plot.Left, y, plot.Right, y);
            g.DrawString(t.ToString("HH:mm"), smallFont, textBrush, 4, y - 8);
        }

        var gap = Math.Max(8, plot.Width / 100);
        var barWidth = Math.Max(24, (plot.Width - gap * (_stations.Length + 1)) / _stations.Length);

        for (var i = 0; i < _stations.Length; i++)
        {
            var station = _stations[i];
            var x = plot.Left + gap + i * (barWidth + gap);
            var bar = new Rectangle(x, plot.Top, barWidth, plot.Height);

            FillSegment(g, runBrush, bar, start, elapsedEnd, start, totalMinutes, plot);
            FillSegment(g, futureBrush, bar, elapsedEnd, end, start, totalMinutes, plot);

            DrawStopSegments(g, stopBrush, bar, station, start, end, totalMinutes, plot);
            DrawPlannedSegments(g, planBrush, bar, start, end, totalMinutes, plot);

            g.DrawRectangle(Pens.White, bar);
            var stationSize = g.MeasureString(station, smallFont);
            g.DrawString(station, smallFont, textBrush, x + (barWidth - stationSize.Width) / 2, plot.Top - 18);
        }
    }

    private void DrawLegend(Graphics g, Font font, Brush textBrush, Brush run, Brush plan, Brush stop, Brush future)
    {
        var x = ClientSize.Width - 330;
        DrawLegendItem(g, run, textBrush, font, x, 16, "가동");
        DrawLegendItem(g, plan, textBrush, font, x + 72, 16, "계획 정지");
        DrawLegendItem(g, stop, textBrush, font, x + 166, 16, "비가동");
        DrawLegendItem(g, future, textBrush, font, x + 240, 16, "미작업시간");
    }

    private static void DrawLegendItem(Graphics g, Brush fill, Brush text, Font font, int x, int y, string label)
    {
        g.FillRectangle(fill, x, y + 2, 14, 10);
        g.DrawString(label, font, text, x + 18, y - 1);
    }

    private void DrawPlannedSegments(Graphics g, Brush brush, Rectangle bar, DateTime start, DateTime end, double totalMinutes, Rectangle plot)
    {
        if (_planned is null || !_planned.Columns.Contains("from_time") || !_planned.Columns.Contains("to_time"))
        {
            return;
        }

        foreach (DataRow row in _planned.Rows)
        {
            var from = CombineTime(start, Convert.ToString(row["from_time"]));
            var to = CombineTime(start, Convert.ToString(row["to_time"]));
            if (to < from)
            {
                to = to.AddDays(1);
            }
            FillSegment(g, brush, bar, from, to, start, totalMinutes, plot, end);
        }
    }

    private void DrawStopSegments(Graphics g, Brush brush, Rectangle bar, string station, DateTime start, DateTime end, double totalMinutes, Rectangle plot)
    {
        if (_nonOperation is null)
        {
            return;
        }

        foreach (DataRow row in _nonOperation.Rows)
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
                FillSegment(g, brush, bar, from, to, start, totalMinutes, plot, end);
            }
        }
    }

    private static void FillSegment(Graphics g, Brush brush, Rectangle bar, DateTime from, DateTime to, DateTime start, double totalMinutes, Rectangle plot, DateTime? endLimit = null)
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
        var rect = new Rectangle(bar.Left, y1, bar.Width, Math.Max(1, y2 - y1));
        g.FillRectangle(brush, rect);
    }

    private static int TimeToY(DateTime time, DateTime start, double totalMinutes, Rectangle plot)
    {
        var ratio = Math.Clamp((time - start).TotalMinutes / totalMinutes, 0, 1);
        return plot.Top + (int)Math.Round(plot.Height * ratio);
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
}
