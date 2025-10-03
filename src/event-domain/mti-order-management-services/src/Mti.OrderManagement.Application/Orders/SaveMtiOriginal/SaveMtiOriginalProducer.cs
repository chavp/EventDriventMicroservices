using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Mti.Domain.Core.Guards;
using Mti.Domain.Infrastructure.Common;
using Mti.OrderManagement.Application.Orders.SaveProducts;
using Mti.OrderManagement.Contracts.Orders;
using Mti.PartyManagement.Messaging;
using Mti.ProductManagement.Messaging.Products.Commands;
using Newtonsoft.Json;

namespace Mti.OrderManagement.Application.Orders.SaveMtiOriginal
{
    public sealed class SaveMtiOriginalProducer : BackgroundService
    {
        private readonly ILogger _logger;
        private readonly string _stateTopic;
        private readonly string _bootstrapServers;
        private readonly IProducer<Guid, MtiOriginalOrderResponse> _producer;

        private readonly Channel<MtiOriginalOrderResponse> _requestChannel;

        public SaveMtiOriginalProducer(
            IConfiguration configuration,
            ILogger<SaveMtiOriginalProducer> logger,
            Channel<MtiOriginalOrderResponse> requestChannel)
        {
            Ensure.NotNull(configuration,
                "Configuration is required for Kafka producer.",
                "config configuration");

            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _requestChannel = requestChannel
                ?? throw new ArgumentNullException(nameof(requestChannel));

            _bootstrapServers = configuration["Kafka:BootstrapServers"];
            _stateTopic = configuration["Kafka:SaveMtiOriginal:Producer:StateTopic"];

            Ensure.NotNull(_stateTopic,
                "BootstrapServers configuration is required for Kafka producer.",
                "config Kafka:BootstrapServers");
            Ensure.NotNull(_bootstrapServers,
                "RequestTopic configuration is required for Kafka producer.",
                "config Kafka:SaveMtiOriginal:Producer:StateTopic");

            _producer = new ProducerBuilder<Guid, MtiOriginalOrderResponse>(new ProducerConfig
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
            .SetValueSerializer(new JsonSerializer<MtiOriginalOrderResponse>())
            .SetKeySerializer(new JsonSerializer<Guid>())
            .Build();
        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (await _requestChannel.Reader.WaitToReadAsync(stoppingToken))
            {
                var request = await _requestChannel.Reader.ReadAsync(stoppingToken);
                _logger.LogDebug($"read channel SaveMtiOriginalProducer = {JsonConvert.SerializeObject(request)}");

                try
                {
                    var message = new Message<Guid, MtiOriginalOrderResponse>
                    {
                        Key = request.OrderId.Value,
                        Value = request
                    };

                    var deliveryResult = await _producer
                            .ProduceAsync(_stateTopic, message, stoppingToken);
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
