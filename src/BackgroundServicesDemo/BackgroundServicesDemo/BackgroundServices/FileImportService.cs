
using BackgroundServicesDemo.Contracts;
using System.Threading.Channels;

namespace BackgroundServicesDemo.BackgroundServices
{
    public class FileImportService : BackgroundService
    {
        private readonly Channel<FileImportRequest> _channel;

        public FileImportService(Channel<FileImportRequest> channel)
        {
            _channel = channel;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Run(async () =>
            {
                while (_channel.Reader.CanPeek)
                {
                    var request = await _channel.Reader.ReadAsync(stoppingToken);
                    File.WriteAllBytes("D:\\github\\EventDriventMicroservices\\src\\" + request.FileName, request.FileContent);
                }
            });
        }
    }
}
