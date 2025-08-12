namespace BackgroundServicesDemo.Contracts
{
    public class FileImportRequest
    {
        public string? FileName { get; set; }
        public byte[]? FileContent { get; set; }
    }
}
