
using System.Threading.Channels;

namespace ChannelsDemo
{
    public class Processor : BackgroundService
    {
        private readonly ILogger _logger;
        private readonly Channel<ChannelRequest> _channel;

        public Processor(ILogger<Processor> logger,
            Channel<ChannelRequest> channel)
        {
            _logger = logger;
            _channel = channel;
        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //while (!stoppingToken.IsCancellationRequested)
            //{
            //    //await Task.Delay(1000);
            //    //_logger.LogInformation(DateTime.Now.ToString());
            //}
            while (await _channel.Reader.WaitToReadAsync(stoppingToken))
            {
                var request = await _channel.Reader.ReadAsync(stoppingToken);
                _logger.LogInformation(request.Message);
            }
        }
    }

    public record ChannelRequest(string Message);
}
