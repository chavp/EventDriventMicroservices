
namespace BackgroundServicesDemo.BackgroundServices
{
    public class SampleService1 : IHostedService
    {
        private readonly ILogger _logger;
        private Timer? _timer;

        public SampleService1(ILogger<SampleService1> logger)
        {
            _logger = logger;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _timer = new Timer(_timer_callback,
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1));
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _timer?.Dispose();
            return Task.CompletedTask;
        }

        private void _timer_callback(object? state)
        {
            _logger.LogInformation("SampleService1 - 1s");
        }
    }
}
