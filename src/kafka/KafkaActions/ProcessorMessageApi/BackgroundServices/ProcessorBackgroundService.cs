
using System.Net;
using System.Threading.Channels;
using Confluent.Kafka;
using Send.Messaging;
using Send.Messaging.Common;
using Send.Messaging.Send;

namespace ProcessorMessageApi.BackgroundServices
{
    public class ProcessorBackgroundService : BackgroundService
    {
        private readonly ILogger<ProcessorBackgroundService> _logger;
        private readonly Channel<SendRequest> _channel;
        private readonly IConfiguration _configuration;

        private readonly string _requestTopic = "send_request";
        private readonly string _responseTopic = "send_response";
        private readonly ConsumerBuilder<Guid, SendRequest> _consumerBuilder;
        private readonly ProducerBuilder<Guid, SendResponse> _producerBuilder;

        public ProcessorBackgroundService(
            IConfiguration configuration,
            ILogger<ProcessorBackgroundService> logger)
        {
            _configuration = configuration;
            _logger = logger;

            _requestTopic = _configuration["Kafka:RequestTopic"] ?? _requestTopic;
            _responseTopic = _configuration["Kafka:ResponseTopic"] ?? _responseTopic;

            _consumerBuilder = new ConsumerBuilder<Guid, SendRequest>(new ConsumerConfig
                {
                    BootstrapServers = _configuration["Kafka:BootstrapServers"],
                    GroupId = _configuration["Kafka:GroupId"],
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                })
                .SetValueDeserializer(new JsonDeserializer<SendRequest>())
                .SetKeyDeserializer(new JsonDeserializer<Guid>());

            _producerBuilder = new ProducerBuilder<Guid, SendResponse>(new ProducerConfig
                {
                    BootstrapServers = _configuration["Kafka:BootstrapServers"],
                    ClientId = Dns.GetHostName(),
                    EnableIdempotence = true,
                    Acks = Acks.All,
                })
                .SetValueSerializer(new JsonSerializer<SendResponse>())
                .SetKeySerializer(new JsonSerializer<Guid>());

        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Run(() => startConsume(stoppingToken), stoppingToken);
        }

        protected async Task processRequestAsync(Guid key, SendRequest request, CancellationToken stoppingToken)
        {
            var genId = Guid.NewGuid();
            var resp = new SendResponse(genId, "Me");

            using (var producer = _producerBuilder.Build())
            {
                try
                {
                    var message = new Message<Guid, SendResponse>
                    {
                        Key = key,
                        Value = resp
                    };
                    var deliveryResult = await producer
                            .ProduceAsync(_responseTopic, message, stoppingToken);
                    _logger.LogInformation($"Message sent to partition {deliveryResult.Partition} with offset {deliveryResult.Offset}, key {key}");
                }
                catch (ProduceException<Null, SendRequest> ex)
                {
                    _logger.LogError(ex, "Failed to send message");
                }
            }
        }

        private async Task startConsume(CancellationToken stoppingToken)
        {
            using (var consumer = _consumerBuilder.Build())
            {
                consumer.Subscribe(_requestTopic);
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(stoppingToken);

                        // process consumed message
                        await processRequestAsync(cr.Message.Key, cr.Message.Value, stoppingToken);
                        _logger.LogInformation($"Consumed message '{cr.Message.Value.Message}' at: {DateTimeOffset.Now}, key: {cr.Key}");
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
