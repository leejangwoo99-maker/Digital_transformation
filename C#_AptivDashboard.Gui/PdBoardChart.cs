using System.Data;
using System.Globalization;
using System.Text.RegularExpressions;

namespace AptivDashboard.Gui;

internal sealed class PdBoardChart : Control
{
    private readonly List<Series> _series = [];
    private double? _threshold;

    public PdBoardChart()
    {
        DoubleBuffered = true;
        BackColor = Color.White;
    }

    public void SetData(DataTable? table)
    {
        _series.Clear();
        _threshold = null;

        if (table is null || !table.Columns.Contains("station") || !table.Columns.Contains("cosine_similarity"))
        {
            Invalidate();
            return;
        }

        foreach (DataRow row in table.Rows)
        {
            var station = Convert.ToString(row["station"]) ?? "";
            var raw = Convert.ToString(row["cosine_similarity"]) ?? "";
            var parsed = ParseCosineData(station, raw);
            if (parsed.Points.Count > 0)
            {
                _series.Add(parsed);
                _threshold ??= parsed.Threshold;
            }
        }

        Invalidate();
    }

    protected override void OnPaint(PaintEventArgs e)
    {
        base.OnPaint(e);
        var g = e.Graphics;
        g.SmoothingMode = System.Drawing.Drawing2D.SmoothingMode.AntiAlias;

        using var titleFont = new Font(Font.FontFamily, 10, FontStyle.Regular);
        using var smallFont = new Font(Font.FontFamily, 8);
        using var textBrush = new SolidBrush(Color.FromArgb(45, 45, 45));
        using var gridPen = new Pen(Color.FromArgb(226, 226, 226));
        using var axisPen = new Pen(Color.FromArgb(120, 120, 120));

        g.DrawString("Cosine Similarity to Abnormal Reference", titleFont, textBrush, 12, 8);

        var plot = new Rectangle(54, 72, Math.Max(80, ClientSize.Width - 74), Math.Max(80, ClientSize.Height - 102));
        g.DrawRectangle(axisPen, plot);

        if (_series.Count == 0)
        {
            return;
        }

        var allPoints = _series.SelectMany(s => s.Points).ToList();
        var labels = allPoints.Select(p => p.Label).Distinct().ToList();
        if (labels.Count == 0)
        {
            return;
        }

        var minY = Math.Min(-0.6, allPoints.Min(p => p.Value));
        var maxY = Math.Max(0.8, Math.Max(allPoints.Max(p => p.Value), _threshold ?? 0.7));
        if (Math.Abs(maxY - minY) < 0.001)
        {
            maxY += 1;
            minY -= 1;
        }

        for (var i = 0; i <= 5; i++)
        {
            var yValue = minY + (maxY - minY) * i / 5;
            var y = ValueToY(yValue, minY, maxY, plot);
            g.DrawLine(gridPen, plot.Left, y, plot.Right, y);
            g.DrawString(yValue.ToString("0.0", CultureInfo.InvariantCulture), smallFont, textBrush, 8, y - 8);
        }

        for (var i = 0; i < labels.Count; i++)
        {
            var x = IndexToX(i, labels.Count, plot);
            g.DrawLine(gridPen, x, plot.Top, x, plot.Bottom);
            g.DrawString(ShortLabel(labels[i]), smallFont, textBrush, x - 18, plot.Bottom + 6);
        }

        DrawThreshold(g, plot, minY, maxY);
        DrawSeries(g, plot, labels, minY, maxY);
        DrawLegend(g, smallFont, textBrush);
    }

    private void DrawThreshold(Graphics g, Rectangle plot, double minY, double maxY)
    {
        if (_threshold is null)
        {
            return;
        }

        using var pen = new Pen(Color.FromArgb(92, 150, 185), 2) { DashStyle = System.Drawing.Drawing2D.DashStyle.Dash };
        var y = ValueToY(_threshold.Value, minY, maxY, plot);
        g.DrawLine(pen, plot.Left, y, plot.Right, y);
    }

    private void DrawSeries(Graphics g, Rectangle plot, List<string> labels, double minY, double maxY)
    {
        var colors = new[]
        {
            Color.FromArgb(31, 119, 180),
            Color.FromArgb(255, 127, 14),
            Color.FromArgb(44, 160, 44),
            Color.FromArgb(214, 39, 40),
            Color.FromArgb(148, 103, 189),
            Color.FromArgb(140, 86, 75),
        };

        for (var s = 0; s < _series.Count; s++)
        {
            var series = _series[s];
            using var pen = new Pen(colors[s % colors.Length], 2);
            using var brush = new SolidBrush(colors[s % colors.Length]);
            var points = series.Points
                .Select(p => new PointF(IndexToX(labels.IndexOf(p.Label), labels.Count, plot), ValueToY(p.Value, minY, maxY, plot)))
                .ToArray();

            if (points.Length > 1)
            {
                g.DrawLines(pen, points);
            }

            foreach (var point in points)
            {
                g.FillEllipse(brush, point.X - 3, point.Y - 3, 6, 6);
            }
        }
    }

    private void DrawLegend(Graphics g, Font font, Brush textBrush)
    {
        var colors = new[]
        {
            Color.FromArgb(31, 119, 180),
            Color.FromArgb(255, 127, 14),
            Color.FromArgb(44, 160, 44),
            Color.FromArgb(214, 39, 40),
            Color.FromArgb(148, 103, 189),
            Color.FromArgb(140, 86, 75),
        };
        var x = 12;
        const int y = 42;
        for (var i = 0; i < _series.Count; i++)
        {
            using var pen = new Pen(colors[i % colors.Length], 2);
            g.DrawLine(pen, x, y + 8, x + 24, y + 8);
            g.DrawString(_series[i].Name, font, textBrush, x + 28, y);
            x += Math.Max(70, TextRenderer.MeasureText(_series[i].Name, font).Width + 42);
        }

        if (_threshold is not null)
        {
            using var pen = new Pen(Color.FromArgb(92, 150, 185), 2) { DashStyle = System.Drawing.Drawing2D.DashStyle.Dash };
            g.DrawLine(pen, x, y + 8, x + 24, y + 8);
            g.DrawString("cos_th", font, textBrush, x + 28, y);
        }
    }

    private static Series ParseCosineData(string station, string raw)
    {
        var labels = ExtractArray(raw, "x");
        var values = ExtractArray(raw, "y")
            .Select(v => double.TryParse(v, NumberStyles.Float, CultureInfo.InvariantCulture, out var n) ? n : double.NaN)
            .Where(v => !double.IsNaN(v))
            .ToList();
        var threshold = ExtractNumber(raw, "th");
        var count = Math.Min(labels.Count, values.Count);

        var points = new List<PointValue>();
        for (var i = 0; i < count; i++)
        {
            points.Add(new PointValue(labels[i], values[i]));
        }

        return new Series(station, points, threshold);
    }

    private static List<string> ExtractArray(string raw, string key)
    {
        var match = Regex.Match(raw, $@"['""]{Regex.Escape(key)}['""]\s*:\s*\[(?<items>[^\]]*)\]");
        if (!match.Success)
        {
            return [];
        }

        return match.Groups["items"].Value
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(v => v.Trim().Trim('\'', '"'))
            .Where(v => v.Length > 0)
            .ToList();
    }

    private static double? ExtractNumber(string raw, string key)
    {
        var match = Regex.Match(raw, $@"['""]{Regex.Escape(key)}['""]\s*:\s*(?<value>-?\d+(?:\.\d+)?)");
        return match.Success && double.TryParse(match.Groups["value"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var n)
            ? n
            : null;
    }

    private static string ShortLabel(string label) => label.Length == 8 && label.All(char.IsDigit)
        ? $"{label[4..6]}-{label[6..8]}"
        : label;

    private static float IndexToX(int index, int count, Rectangle plot)
    {
        if (count <= 1)
        {
            return plot.Left + plot.Width / 2f;
        }
        return plot.Left + plot.Width * index / (float)(count - 1);
    }

    private static float ValueToY(double value, double minY, double maxY, Rectangle plot)
    {
        var ratio = (value - minY) / (maxY - minY);
        return plot.Bottom - (float)(plot.Height * ratio);
    }

    private sealed record Series(string Name, List<PointValue> Points, double? Threshold);

    private sealed record PointValue(string Label, double Value);
}
