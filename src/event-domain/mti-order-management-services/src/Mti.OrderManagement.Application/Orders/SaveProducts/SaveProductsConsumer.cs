using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Mti.Domain.Infrastructure.Common;
using Mti.OrderManagement.Domain.Orders;
using Mti.OrderManagement.Persistence;
using Mti.ProductManagement.Messaging.Products.Commands;

namespace Mti.OrderManagement.Application.Orders.SaveProducts
{
    public sealed class SaveProductConsumer : BackgroundService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger _logger;

        private readonly IDbContextFactory<OrdersContext> _dbContextFactory;

        private readonly string _bootstrapServers;
        private readonly string _responseTopic;
        private readonly string _groupId;
        private readonly IConsumer<Guid, SaveProductsByOrderResponse> _consumer;

        public SaveProductConsumer(IConfiguration configuration,
            ILogger<SaveProductConsumer> logger,
            IDbContextFactory<OrdersContext> dbContextFactory)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _dbContextFactory = dbContextFactory ?? throw new ArgumentNullException(nameof(dbContextFactory));

            _bootstrapServers = configuration["Kafka:BootstrapServers"] ?? throw new ArgumentNullException("Required Kafka:BootstrapServers");
            _responseTopic = configuration["Kafka:SaveProducts:Consumer:ResponseTopic"] ?? throw new ArgumentNullException("Required Kafka:SaveProduct:Consumer:ResponseTopic");
            _groupId = configuration["Kafka:SaveProducts:Consumer:GroupId"] ?? throw new ArgumentNullException("Required Kafka:SaveProduct:Consumer:GroupId");

            _consumer = new ConsumerBuilder<Guid, SaveProductsByOrderResponse>(new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            })
            .SetValueDeserializer(new JsonDeserializer<SaveProductsByOrderResponse>())
            .SetKeyDeserializer(new JsonDeserializer<Guid>())
            .Build();
        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Run(() => startConsume(stoppingToken), stoppingToken);
        }

        private async Task startConsume(CancellationToken stoppingToken)
        {
            _consumer.Subscribe(_responseTopic);
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var cr = _consumer.Consume(stoppingToken);
                    if (cr == null || cr.Message == null)
                    {
                        continue;
                    }

                    var response = cr.Message.Value;
                    // process consumed message
                    _logger.LogDebug($"Begin Consumed message '{response}' at: {DateTimeOffset.Now}, key: {cr.Message.Key}");

                    using(var db = _dbContextFactory.CreateDbContext())
                    using(var tran = db.Database.BeginTransaction()) 
                    {
                        var saleOrder = db.Orders
                            .OfType<MtiOriginalSalesOrder>()
                            .Include(o => o.Items)
                            .SingleOrDefault(x => x.Id == response.OrderId);
                        if(saleOrder != null)
                        {
                            saleOrder.Products_TenantId = response.Products_TenantId;
                            foreach (var item in saleOrder.Items)
                            {
                                var respItem = response.OrderItems.SingleOrDefault(x => x.OrderItemId == item.Id);
                                if (respItem != null)
                                {
                                    if(respItem.Product != null)
                                    {
                                        item.Products_ProductId = respItem.Product.ProductId;
                                    }
                                    if (respItem.Coverage != null)
                                    {
                                        item.Products_CoverageTypeId = respItem.Coverage.CoverageTypeId;
                                        item.Products_CoverageLevelId = respItem.Coverage.CoverageLevelId;

                                    }
                                    if (respItem.ProductFeature != null)
                                    {
                                        item.Products_ProductFeatureId = respItem.ProductFeature.ProductFeatureId;
                                    }
                                }

                            }

                            await db.SaveChangesAsync(stoppingToken);
                            await tran.CommitAsync(stoppingToken);
                        }

                    }
                    
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, $"Error consuming: {ex.Error.Reason}");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An error occurred while consuming messages");
                }
            }
            _consumer.Close();
        }
    }
}
