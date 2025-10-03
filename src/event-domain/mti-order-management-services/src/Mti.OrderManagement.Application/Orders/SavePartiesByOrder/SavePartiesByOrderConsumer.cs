using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Mti.Domain.Infrastructure.Common;
using Mti.Domain.Messaging.SavePartiesByOrder;
using Mti.Domain.Messaging.SaveProductByOrder;
using Mti.OrderManagement.Domain.Orders;
using Mti.OrderManagement.Persistence;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Mti.OrderManagement.Application.Orders.SavePartiesByOrder
{
    public sealed class SavePartiesByOrderConsumer : BackgroundService
    {
        private readonly ILogger _logger;
        private readonly IConfiguration _configuration;

        private readonly IDbContextFactory<OrdersContext> _dbContextFactory;
        private readonly string _responseTopic = "save_parties_by_order_response";
        private readonly IConsumer<Guid, SavePartiesByOrderResponse> _consumer;
        
        public SavePartiesByOrderConsumer(
            IConfiguration configuration,
            ILogger<SavePartiesByOrderConsumer> logger,
            IDbContextFactory<OrdersContext> dbContextFactory)
        {
            _configuration = configuration;
            _logger = logger;
            _dbContextFactory = dbContextFactory;

            _responseTopic = _configuration["Kafka:SavePartiesByOrder:ResponseTopic"] ?? _responseTopic;

            _consumer = new ConsumerBuilder<Guid, SavePartiesByOrderResponse>(new ConsumerConfig
            {
                BootstrapServers = _configuration["Kafka:BootstrapServers"],
                GroupId = _configuration["Kafka:SavePartiesByOrder:GroupId"],
                AutoOffsetReset = AutoOffsetReset.Earliest,
            })
            .SetValueDeserializer(new JsonDeserializer<SavePartiesByOrderResponse>())
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
                    if(cr == null || cr.Message == null)
                    {
                        continue;
                    }

                    var response = cr.Message.Value;
                    // process consumed message
                    _logger.LogInformation($"Begin Consumed message '{response}' at: {DateTimeOffset.Now}, key: {cr.Message.Key}");
                    await consumeSavePartiesByOrderResponse(response);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, $"Error consuming: {ex.Error.Reason}");
                }
                catch(Exception ex)
                {
                    _logger.LogError(ex, "An error occurred while consuming messages");
                }
            }
            _consumer.Close();
        }

        private async Task consumeSavePartiesByOrderResponse(SavePartiesByOrderResponse savePartiesByOrderResp)
        {
            using (var ordDb = _dbContextFactory.CreateDbContext())
            using (var tran = ordDb.Database.BeginTransaction())
            {
                var saleOrder = ordDb.Orders.OfType<MtiOriginalSalesOrder>()
                        .Include(x => x.Items)
                        .Single(x => x.Id == savePartiesByOrderResp.OrderId);
                saleOrder.Parties_TenantId = savePartiesByOrderResp.Parties_TenantId;
                ordDb.SaveChanges();

                foreach (var savePartiesByOrderItem in savePartiesByOrderResp.SaveRoleOrderItems)
                {
                    var saleOrderItem = ordDb.OrderItems
                            .OfType<MtiOriginalSalesOrderItem>()
                            .Include(x => x.Roles)
                            .Include(x => x.InsuredAsset)
                        .SingleOrDefault(x => x.Id == savePartiesByOrderItem.OrderItemId);
                    if(saleOrderItem == null)
                    {
                        _logger.LogWarning($"Order item with ID {savePartiesByOrderItem.OrderItemId} not found for order {savePartiesByOrderResp.OrderId}");
                        continue;
                    }

                    if (saleOrderItem.Roles.Any())
                    {
                        // remove all roles
                        ordDb.RemoveRange(saleOrderItem.Roles);
                        ordDb.SaveChanges();
                    }
                    if (saleOrderItem.InsuredAssetId.HasValue)
                    {
                        saleOrderItem.InsuredAsset.Parties_AssetId = null;
                        if (savePartiesByOrderItem.Asset != null)
                        {
                            saleOrderItem.InsuredAsset.Parties_AssetId = savePartiesByOrderItem.Asset.AssetId;
                        }

                        ordDb.SaveChanges();
                    }

                    // add order item role parties
                    foreach (var partyProfile in savePartiesByOrderItem.Parties)
                    {
                        var orderItemRoleType = ordDb
                            .OrderRoleTypes
                            .Single(x => x.Code == partyProfile.PartyRoleTypeCode);

                        var roleParty = new OrderItemRole(saleOrderItem.Id, orderItemRoleType.Id, partyProfile.PartyId);
                        ordDb.Add(roleParty);
                        ordDb.SaveChanges();
                    }
                }

                tran.Commit();
            }
        }

    }
}
