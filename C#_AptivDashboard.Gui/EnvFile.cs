namespace AptivDashboard.Gui;

internal static class EnvFile
{
    public static string Get(string key, string defaultValue = "")
    {
        var fromProcess = Environment.GetEnvironmentVariable(key);
        if (!string.IsNullOrWhiteSpace(fromProcess))
        {
            return fromProcess.Trim();
        }

        foreach (var path in CandidatePaths())
        {
            if (!File.Exists(path))
            {
                continue;
            }

            foreach (var line in File.ReadLines(path))
            {
                var trimmed = line.Trim();
                if (trimmed.Length == 0 || trimmed.StartsWith("#", StringComparison.Ordinal))
                {
                    continue;
                }

                var idx = trimmed.IndexOf('=');
                if (idx <= 0)
                {
                    continue;
                }

                var name = trimmed[..idx].Trim();
                if (!string.Equals(name, key, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                return Unquote(trimmed[(idx + 1)..].Trim());
            }
        }

        return defaultValue;
    }

    private static IEnumerable<string> CandidatePaths()
    {
        yield return Path.Combine(AppContext.BaseDirectory, ".env");
        yield return Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".env");
        yield return @"C:\Users\user\PycharmProjects\PythonProject\app\.env";
        yield return @"C:\Users\user\PycharmProjects\PythonProject\.env";
    }

    private static string Unquote(string value)
    {
        if (value.Length >= 2)
        {
            var first = value[0];
            var last = value[^1];
            if ((first == '"' && last == '"') || (first == '\'' && last == '\''))
            {
                return value[1..^1];
            }
        }

        return value;
    }
}
