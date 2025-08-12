
using System.Threading;
using System.Threading.Channels;

namespace BackgroundServicesDemo.BackgroundServices
{
    public class SampleService2 : BackgroundService
    {
        private readonly ILogger _logger;
        private Timer? _timer;
        private Channel<string> _channel;

        public SampleService2(ILogger<SampleService2> logger)
        {
            _logger = logger;

            _channel = Channel.CreateUnbounded<string>();
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _timer = new Timer(_timer_callback,
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1));

            //while (!stoppingToken.IsCancellationRequested)
            //{
            //    Task.Delay(TimeSpan.FromSeconds(1)).Wait();
            //    _logger.LogInformation("SampleService2 - 1s");
            //}

            Task.Run(() =>
            {
                while (_channel.Reader.CanPeek)
                {
                    if(_channel.Reader.TryRead(out var message))
                        _logger.LogInformation("Receive message: {message}", message);
                }
            });

            return Task.CompletedTask;
        }

        private void _timer_callback(object? state)
        {
            //_logger.LogInformation("SampleService2 - 1s");

            var sampleMessage = Guid.NewGuid().ToString();
            _logger.LogInformation("Sending Message: {message}", sampleMessage);
            _channel.Writer.WriteAsync(sampleMessage);
        }
    }
}
