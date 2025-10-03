namespace Mti.PartyManagement.Services.Api
{
    public static class DataExtensions
    {
        public static string? CleanNull(this string? data)
        {
            if (string.IsNullOrEmpty(data))
                return null;
            data = data.Trim()
                .Replace("NULL", "")
                .Replace("-", "")
                .Replace("UNKNOWN", "")
                ;
            if (string.IsNullOrEmpty(data)) return null;
            return data;
        }
        public static string? GenCode(this string? data)
        {
            if (string.IsNullOrEmpty(data))
                return null;
            data = data.Trim();
            var codes = data.Split(" ", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (!codes.Any()) return null;

            var code = "#" + string.Join("_", codes).ToUpperInvariant();
            return code;
        }
    }
}
