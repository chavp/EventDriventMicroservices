
using System.Net;
using Ardalis.GuardClauses;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Mti.ProductManagement.Application.Products.SaveProductMtiOriginal;
using Mti.ProductManagement.Messaging.Products.Commands;
using Mti.ProductManagement.Persistance;
using Mti.ProductManagement.Services.Api.Infrastructure;

namespace Mti.ProductManagement.Services.Api.BackgroundServices
{
    public class SaveProductsByOrderService : BackgroundService
    {
        private readonly ILogger<SaveProductsByOrderService> _logger;

        private readonly string _bootstrapServers;
        private readonly string _groupId;
        private readonly string _requestTopic;
        private readonly string _responseTopic;

        private ConsumerBuilder<Guid, SaveProductsByOrderRequest> _consumerBuilder;
        private ProducerBuilder<Guid, SaveProductsByOrderResponse> _producerBuilder;

        protected readonly IDbContextFactory<ProductsContext> _dbFactory = null;
        protected readonly ILoggerFactory _loggerFactory;
        public SaveProductsByOrderService(
            IConfiguration configuration,
            ILoggerFactory loggerFactory,
            IDbContextFactory<ProductsContext> dbFactory
            )
        {
            Guard.Against.Null(configuration);
            _loggerFactory = Guard.Against.Null(loggerFactory);
            _logger = _loggerFactory.CreateLogger<SaveProductsByOrderService>();
            _dbFactory = Guard.Against.Null(dbFactory);

            _bootstrapServers = Guard.Against.NullOrEmpty(configuration["Kafka:BootstrapServers"]);

            _requestTopic = Guard.Against.NullOrEmpty(configuration["Kafka:SaveProductsByOrderService:RequestTopic"]);
            _responseTopic = Guard.Against.NullOrEmpty(configuration["Kafka:SaveProductsByOrderService:ResponseTopic"]);
            _groupId = Guard.Against.NullOrEmpty(configuration["Kafka:SaveProductsByOrderService:GroupId"]);

            _consumerBuilder = new ConsumerBuilder<Guid, SaveProductsByOrderRequest>(new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
            })
            .SetValueDeserializer(new JsonDeserializer<SaveProductsByOrderRequest>())
            .SetKeyDeserializer(new JsonDeserializer<Guid>());

            _producerBuilder = new ProducerBuilder<Guid, SaveProductsByOrderResponse>(new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                ClientId = Dns.GetHostName(),
                EnableIdempotence = true,
                Acks = Acks.All,
            })
            .SetValueSerializer(new JsonSerializer<SaveProductsByOrderResponse>())
            .SetKeySerializer(new JsonSerializer<Guid>());
        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Run(() => startConsume(stoppingToken), stoppingToken);
        }
        private async Task startConsume(CancellationToken stoppingToken)
        {
            using (var consumer = _consumerBuilder.Build())
            using (var producer = _producerBuilder.Build())
            {
                consumer.Subscribe(_requestTopic);
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(stoppingToken);
                        if (cr == null || cr.Message == null)
                        {
                            continue;
                        }
                        var request = cr.Message.Value;
                        var key = cr.Message.Key;
                        _logger.LogInformation("Received request for OrderId: {OrderId}", request.OrderId);
                        var saveProductCommand = new SaveProductMtiOriginalCommand(request);
                        var saveProductCommandHandler = new SaveProductMtiOriginalCommandHandler(
                            _loggerFactory.CreateLogger<SaveProductMtiOriginalCommandHandler>(),
                            _dbFactory);
                        var result = await saveProductCommandHandler.Handle(saveProductCommand, stoppingToken);
                        if (result.IsSuccess)
                        {
                            await producer.ProduceAsync(_responseTopic, new Message<Guid, SaveProductsByOrderResponse>
                            {
                                Key = key,
                                Value = result.Value
                            }, stoppingToken);
                            _logger.LogDebug("Saved product event published for product for order id: {OrderId}", result.Value.OrderId);
                        }
                        else
                        {
                            _logger.LogError("Failed to save product: {ErrorMessage}", result.Errors.FirstOrDefault()?.Message);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing message");
                    }
                }
            }
        }

    }
}
