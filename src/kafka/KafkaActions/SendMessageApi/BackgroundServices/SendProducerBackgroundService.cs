using System.IO.Compression;
using System.Net;
using System.Threading.Channels;
using Confluent.Kafka;
using Send.Messaging;
using Send.Messaging.Common;
using Send.Messaging.Send;

namespace SendMessageApi.BackgroundTasks
{
    public class SendProducerBackgroundService : BackgroundService
    {
        private readonly ILogger<SendProducerBackgroundService> _logger;
        private readonly Channel<SendRequest> _channel;
        private readonly IConfiguration _configuration;
        private readonly ProducerBuilder<Guid, SendRequest> _producerBuilder;

        private readonly string _requestTopic = "send_request";
        private readonly string _responseTopic = "send_response";

        public SendProducerBackgroundService(
            IConfiguration configuration,
            ILogger<SendProducerBackgroundService> logger,
            Channel<SendRequest> channel)
        {
            _configuration = configuration;
            _logger = logger;
            _channel = channel;

            var config = new ProducerConfig
            {
                BootstrapServers = _configuration["Kafka:BootstrapServers"],
                ClientId = Dns.GetHostName(),
                EnableIdempotence = true,
                Acks = Acks.All,
            };

            _requestTopic = _configuration["Kafka:RequestTopic"] ?? _requestTopic;
            _responseTopic = _configuration["Kafka:ResponseTopic"] ?? _responseTopic;

            _producerBuilder = new ProducerBuilder<Guid, SendRequest>(config)
                .SetValueSerializer(new JsonSerializer<SendRequest>())
                .SetKeySerializer(new JsonSerializer<Guid>());

        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await TopicHelpers.DeleteKafkaTopics(
                _configuration["Kafka:BootstrapServers"],
                new[] { _requestTopic, _responseTopic });

            await TopicHelpers.CreateKafkaTopicAsync(
                _configuration["Kafka:BootstrapServers"],
                _requestTopic,
                -1, -1);

            await TopicHelpers.CreateKafkaTopicAsync(
                _configuration["Kafka:BootstrapServers"],
                _responseTopic,
                -1, -1);

            while (await _channel.Reader.WaitToReadAsync(stoppingToken))
            {
                var request = await _channel.Reader.ReadAsync(stoppingToken);
                //_logger.LogInformation(request.Message);
                using(var producer = _producerBuilder.Build())
                {
                    try
                    {
                        var key = Guid.NewGuid();
                        var message = new Message<Guid, SendRequest>
                        {
                            Key = key,
                            Value = request
                        };
                        var deliveryResult = await producer
                                .ProduceAsync(_requestTopic, message, stoppingToken);
                        _logger.LogInformation($"Message sent to partition {deliveryResult.Partition} with offset {deliveryResult.Offset}, key {key}");
                    }
                    catch (ProduceException<Null, SendRequest> ex)
                    {
                        _logger.LogError(ex, "Failed to send message");
                    }
                }
            }
        }
    }


}
