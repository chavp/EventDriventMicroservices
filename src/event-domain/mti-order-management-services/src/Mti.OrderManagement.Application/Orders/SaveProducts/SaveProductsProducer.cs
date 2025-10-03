using System.Net;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Mti.Domain.Core.Guards;
using Mti.Domain.Infrastructure.Common;
using Mti.OrderManagement.Persistence;
using Mti.PartyManagement.Messaging;
using Mti.ProductManagement.Messaging.Products.Commands;
using Newtonsoft.Json;

namespace Mti.OrderManagement.Application.Orders.SaveProducts
{
    public sealed class SaveProductsProducer : BackgroundService
    {
        private readonly ILogger _logger;
        private readonly Channel<SaveProductsByOrderRequest> _saveProductsByOrderRequestChannel;

        private readonly string _bootstrapServers;
        private readonly string _requestTopic;
        private readonly IProducer<Guid, SaveProductsByOrderRequest> _producer;

        public SaveProductsProducer(
            IConfiguration configuration,
            ILogger<SaveProductsProducer> logger,
            Channel<SaveProductsByOrderRequest> saveProductByOrderRequestChannel)
        {
            Ensure.NotNull(configuration,
                "Configuration is required for Kafka producer.",
                "config configuration");

            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _saveProductsByOrderRequestChannel = saveProductByOrderRequestChannel 
                ?? throw new ArgumentNullException(nameof(saveProductByOrderRequestChannel));

            _bootstrapServers = configuration["Kafka:BootstrapServers"];
            _requestTopic = configuration["Kafka:SaveProducts:Producer:RequestTopic"];

            Ensure.NotNull(_requestTopic,
                "BootstrapServers configuration is required for Kafka producer.",
                "config Kafka:BootstrapServers");
            Ensure.NotNull(_bootstrapServers,
                "RequestTopic configuration is required for Kafka producer.",
                "config Kafka:SaveProducts:Producer:RequestTopic");

            _producer = new ProducerBuilder<Guid, SaveProductsByOrderRequest>(new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                ClientId = Dns.GetHostName(),
                EnableIdempotence = true,
                Acks = Acks.All,
            })
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError($"Error in producer: {e.Reason}");
            })
            .SetValueSerializer(new JsonSerializer<SaveProductsByOrderRequest>())
            .SetKeySerializer(new JsonSerializer<Guid>())
            .Build();
        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (await _saveProductsByOrderRequestChannel.Reader.WaitToReadAsync(stoppingToken))
            {
                var request = await _saveProductsByOrderRequestChannel.Reader.ReadAsync(stoppingToken);
                _logger.LogDebug($"request {JsonConvert.SerializeObject(request)}");

                try
                {
                    var message = new Message<Guid, SaveProductsByOrderRequest>
                    {
                        Key = request.OrderId,
                        Value = request
                    };

                    var deliveryResult = await _producer
                            .ProduceAsync(_requestTopic, message, stoppingToken);
                    _logger.LogDebug($"Message sent to partition {deliveryResult.Partition} with offset {deliveryResult.Offset}, key OrderId {request.OrderId}");

                }
                catch (ProduceException<Null, SavePartiesByOrderRequest> ex)
                {
                    _logger.LogError(ex, "Failed to send message");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An unexpected error occurred while sending message");
                    throw;
                }
            }
        }

    }
}
