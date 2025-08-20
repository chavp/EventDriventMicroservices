using Confluent.Kafka;
using Send.Messaging.Common;
using Send.Messaging.Send;

namespace SendMessageApi.BackgroundServices
{
    public class SendProcessorBackgroundService : BackgroundService
    {
        private readonly ILogger<SendProcessorBackgroundService> _logger;
        private readonly IConfiguration _configuration;
        private readonly ConsumerBuilder<Guid, SendResponse> _consumerBuilder;
        private readonly string _responseTopic = "send_response";
        public SendProcessorBackgroundService(
            IConfiguration configuration,
            ILogger<SendProcessorBackgroundService> logger)
        {
            _configuration = configuration;
            _logger = logger;
            var config = new ConsumerConfig
            {
                BootstrapServers = _configuration["Kafka:BootstrapServers"],
                GroupId = _configuration["Kafka:GroupId"],
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            _responseTopic = _configuration["Kafka:ResponseTopic"] ?? _responseTopic;

            _consumerBuilder = new ConsumerBuilder<Guid, SendResponse>(config)
                .SetValueDeserializer(new JsonDeserializer<SendResponse>())
                .SetKeyDeserializer(new JsonDeserializer<Guid>())
                ;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Run(() => startConsume(_responseTopic, stoppingToken), stoppingToken);
        }

        public void startConsume(string topic, CancellationToken stoppingToken)
        {
            using (var consumer = _consumerBuilder.Build())
            {
                consumer.Subscribe(topic);
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(stoppingToken);
                       
                        var value = cr.Message.Value;
                        // process response message
                        _logger.LogInformation($"Consumed Response message '{value.MessageId}' from {value.ProcessBy} at: {DateTimeOffset.Now}, key: {cr.Message.Key}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error consuming: {e.Error.Reason}");
                    }
                }
            }
        }
    }
}
