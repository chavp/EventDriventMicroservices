using System.Net;
using System.Text;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Mti.Domain.Core.Guards;
using Mti.Domain.Infrastructure.Common;
using Mti.OrderManagement.Domain.Orders;
using Mti.OrderManagement.Persistence;
using Mti.PartyManagement.Messaging;

namespace Mti.OrderManagement.Application.Orders.SavePartiesByOrder
{
    public sealed class SavePartiesByOrderProducer
    {
        private readonly ILogger _logger;
        //private readonly Channel<SavePartiesByOrderRequest> _saveMtiOriginalChannel;

        private readonly string _requestTopic;
        private readonly string _responseTopic;
        private readonly string _bootstrapServers ;

        private readonly IProducer<Guid, SavePartiesByOrderRequest> _producer;

        private readonly IDbContextFactory<OrdersContext> _dbContextFactory;


        public SavePartiesByOrderProducer(
            IConfiguration configuration,
            ILogger<SavePartiesByOrderProducer> logger,
            //Channel<SavePartiesByOrderRequest> saveMtiOriginalChannel,
            IDbContextFactory<OrdersContext> dbContextFactory)
        {
            _logger = logger;
            //_saveMtiOriginalChannel = saveMtiOriginalChannel;
            _dbContextFactory = dbContextFactory;

            _requestTopic = configuration["Kafka:SavePartiesByOrder:Producer:RequestTopic"] ?? _requestTopic;
            _responseTopic = configuration["Kafka:SavePartiesByOrder:Producer:ResponseTopic"] ?? _responseTopic;
            _bootstrapServers = configuration["Kafka:BootstrapServers"];
            
            Ensure.NotNull(_bootstrapServers, 
                "BootstrapServers configuration is required for Kafka producer.",
                "config Kafka:BootstrapServers");

            _producer = new ProducerBuilder<Guid, SavePartiesByOrderRequest>(new ProducerConfig
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
            .SetValueSerializer(new JsonSerializer<SavePartiesByOrderRequest>())
            .SetKeySerializer(new JsonSerializer<Guid>())
            .Build();
        }

        //protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        //{
        //    while (await _saveMtiOriginalChannel.Reader.WaitToReadAsync(stoppingToken))
        //    {
        //        var request = await _saveMtiOriginalChannel.Reader.ReadAsync(stoppingToken);
        //        _logger.LogTrace($"request {JsonConvert.SerializeObject(request)}");

        //        await PrcessRequest(request, stoppingToken);
        //    }
        //}

        private IConsumer<Guid, SavePartiesByOrderResponse> buildReplyCunsumer()
        {
            var groupId = $"{_responseTopic}.reply.{Guid.NewGuid()}";
            var consumerBuilder = new ConsumerBuilder<Guid, SavePartiesByOrderResponse>(new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            })
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError($"Error in consumer: {e.Reason}");
            })
            .SetValueDeserializer(new JsonDeserializer<SavePartiesByOrderResponse>())
            .SetKeyDeserializer(new JsonDeserializer<Guid>());
            return consumerBuilder.Build();
        }

        public async Task PrcessRequestAsync(SavePartiesByOrderRequest request, CancellationToken stoppingToken)
        {
            // new reply topic for the response
            var correlationId = Guid.NewGuid();
            //var replyToTopic = $"{correlationId}.reply";

            try
            {
                var message = new Message<Guid, SavePartiesByOrderRequest>
                {
                    Key = request.OrderId,
                    Value = request,
                    Headers = new Headers
                    {
                        { "CorrelationId", Encoding.UTF8.GetBytes(correlationId.ToString()) },
                        //{ "ReplyToTopic", Encoding.UTF8.GetBytes(_responseTopic) }
                    }
                };

                var deliveryResult = await _producer
                        .ProduceAsync(_requestTopic, message, stoppingToken);
                _logger.LogDebug($"Message sent to partition {deliveryResult.Partition} with offset {deliveryResult.Offset}, key OrderId {request.OrderId}");

                using (var consumer = buildReplyCunsumer())
                {
                    consumer.Subscribe(_responseTopic);
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        _logger.LogDebug($"Consume order id = {request.OrderId}, reply = {_responseTopic}, correlationId = {correlationId}");
                        var consumeResult = consumer.Consume(stoppingToken);
                        if(consumeResult == null 
                            || consumeResult.Message == null)
                        {
                            _logger.LogWarning("No message received, retrying...");
                            continue; // Retry if no message is received
                        }

                        if (consumeResult.Message.Headers.TryGetLastBytes("CorrelationId", out var corIdBytes))
                        {
                            var receivedCorrelationId = Encoding.UTF8.GetString(corIdBytes);

                            // Send a response back if needed
                            if (receivedCorrelationId == correlationId.ToString())
                            {
                                if (consumeResult.Message.Headers.TryGetLastBytes("Error", out var headerBytes))
                                {
                                    var error = Encoding.UTF8.GetString(headerBytes);
                                    if (!string.IsNullOrEmpty(error))
                                    {
                                        throw new Exception($"Error received in response: {error}");
                                    }
                                }

                                await consumeSavePartiesByOrderResponse(consumeResult.Message.Value, stoppingToken);
                                break; // Exit the loop if the correlation ID matches
                            }
                        }
                        
                    }
                    consumer.Close();
                }

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

        private async Task consumeSavePartiesByOrderResponse(SavePartiesByOrderResponse savePartiesByOrderResp, CancellationToken stoppingToken)
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
                    if (saleOrderItem == null)
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

                await tran.CommitAsync(stoppingToken);
            }
        }

    }
}
