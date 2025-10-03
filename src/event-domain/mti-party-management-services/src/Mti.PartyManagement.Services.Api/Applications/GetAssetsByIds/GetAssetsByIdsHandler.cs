using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using Ardalis.GuardClauses;
using Confluent.Kafka;
using FluentResults;
using FluentResults.Extensions;
using Microsoft.EntityFrameworkCore;
using Mti.PartyManagement.Domain.Parties;
using Mti.PartyManagement.Messaging.Parties;
using Mti.PartyManagement.Messaging.Parties.GetAssetsByIds;
using Mti.PartyManagement.Persistence;
using Mti.PartyManagement.Services.Api.BackgroundServices.Parties;
using Mti.PartyManagement.Services.Api.Infrastructure;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Mti.PartyManagement.Services.Api.Applications.GetAssetsByIds
{
    public class GetAssetsByIdsHandler 
        : IQueryApplicationHandler<GetAssetsByIdsQuery, GetAssetsByIdsResponse>
    {
        private readonly ILogger<PartyService> _logger;
        protected readonly IDbContextFactory<PartiesContext> _dbFactory = null;
        private readonly string _bootstrapServers;

        private readonly string _requestTopic;
        private readonly string _responseTopic;
        private readonly string _groupId;

        private readonly Channel<MessageWithMetadata<GetAssetsByIdsQuery>> _processingQueue;
        private readonly int _maxParallelTasks = 10;

        private readonly IConsumer<string, GetAssetsByIdsQuery> _consumer;

        private readonly MessageStreamManager<GetAssetsByIdsQuery, GetAssetsByIdsResponse> _messageStreamManager;

        public GetAssetsByIdsHandler(
            IConfiguration configuration,
            ILogger<PartyService> logger,
            IDbContextFactory<PartiesContext> dbFactory)
        {
            _logger = logger;
            _dbFactory = dbFactory;

            _bootstrapServers = Guard.Against.NullOrEmpty(configuration["Kafka:BootstrapServers"]);

            _requestTopic = Guard.Against.NullOrEmpty(configuration["Kafka:GetAssetsByIds:RequestTopic"]);
            _responseTopic = Guard.Against.NullOrEmpty(configuration["Kafka:GetAssetsByIds:ResponseTopic"]);
            _groupId = Guard.Against.NullOrEmpty(configuration["Kafka:GetAssetsByIds:GroupId"]);

            _messageStreamManager = new MessageStreamManager<GetAssetsByIdsQuery, GetAssetsByIdsResponse>(
                _logger, _bootstrapServers, _groupId);

            _consumer = _messageStreamManager
                        .ConsumerBuilder()
                        .Build();

            // สร้าง channel สำหรับ queue ข้อมูล
            var options = new BoundedChannelOptions(100) // queue ได้สูงสุด 100 messages
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = true
            };
            _processingQueue = Channel.CreateBounded<MessageWithMetadata<GetAssetsByIdsQuery>>(options);
        }

        public async Task StartConsumingAsync(CancellationToken stoppingToken)
        {
            await TopicExtensions.CreateKafkaTopicAsync(_bootstrapServers,
                _requestTopic);
            await TopicExtensions.CreateKafkaTopicAsync(_bootstrapServers,
                _responseTopic);

            // เริ่ม worker tasks สำหรับ process ข้อมูล
            var processingTasks = Enumerable.Range(0, _maxParallelTasks)
                .Select(i => processWorkerAsync(i, stoppingToken))
                .ToArray();

            _consumer.Subscribe(_requestTopic);

            _logger.LogDebug($"⚡Producer-Consumer Started (Max Parallel: {_maxParallelTasks})");

            // Consumer loop
            var consumerTask = Task.Run(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, GetAssetsByIdsQuery> cr = null;
                    try
                    {
                        cr = _consumer.Consume(stoppingToken);
                        if (cr != null
                            && cr.Message != null
                            && cr.Message.Headers.TryGetLastBytes("CorrelationId", out var corIdBytes)
                            && cr.Message.Headers.TryGetLastBytes("ReplyTo", out var replyToBytes))
                        {
                            var correlationId = Encoding.UTF8.GetString(corIdBytes);
                            var replyTo = Encoding.UTF8.GetString(replyToBytes);
                            // Process the message
                            var query = cr.Message.Value;

                            var messageWithMetadata = new MessageWithMetadata<GetAssetsByIdsQuery>
                            {
                                CorrelationId = correlationId,
                                ReplyTo = replyTo,
                                Data = query,
                                ConsumeResult = cr,
                            };

                            await _processingQueue.Writer.WriteAsync(messageWithMetadata, stoppingToken);
                        }
                        else
                        {
                            _consumer.Commit(cr);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Consume Error");
                        if (cr != null)
                        {
                            _consumer.Commit(cr);
                        }
                    }
                }
            });

            // รอให้ทุก task เสร็จ
            await Task.WhenAll(new[] { consumerTask }.Concat(processingTasks));

            _consumer.Close();
        }
        public async Task<Result<GetAssetsByIdsResponse>> HandleAsync(
            GetAssetsByIdsQuery query,
            CancellationToken stoppingToken)
        {
            using (var db = _dbFactory.CreateDbContext())
            {
                var assets = await db.Assets
                    .Where(x => query.AssetIds.Contains(x.Id.Value))
                    .AsNoTracking()
                    .ToListAsync(stoppingToken);
                var response = new GetAssetsByIdsResponse();
                var data = new List<AssetMessage>();
                foreach (var asset in assets)
                {
                    if (asset is Vehicle veh)
                    {
                        var newAsset = new AssetMessage("VEHICLE");
                        newAsset.AssetId = asset.Id;
                        data.Add(newAsset);

                        newAsset.Vehicle = new VehicleMessage
                        {
                            Code = veh.Code,
                            Cc = veh.Cc,
                            Chassis = veh.Chassis,
                            Color = veh.Color,
                            Brand = veh.Brand,
                            Model = veh.Model,
                            Engine = veh.Engine,
                            Passenger = veh.Passenger,
                            RegisterNo = veh.RegisterNo,
                            RegisterProvince = veh.RegisterProvince,
                            RegisterYear = veh.RegisterYear,
                            Seat = veh.Seat,
                            Weight = veh.Weight,
                            Tonnage = veh.Tonnage,
                            ManufactoringYear = veh.RegisterYear,
                        };
                    }
                }
                response.Data = data.AsReadOnly();

                // reply

                return Result.Ok(response);
            }
        }

        private async Task processWorkerAsync(int workerId, CancellationToken cancellationToken)
        {
            _logger.LogDebug($"👷 Worker {workerId} started");

            await foreach (var message in _processingQueue.Reader.ReadAllAsync(cancellationToken))
            {
                var correlationId = message.CorrelationId;
                var replyTo = message.ReplyTo;
                var query = message.Data;
                var consumerResult = message.ConsumeResult;
                try
                {
                    _logger.LogDebug($"📦 [Worker-{workerId}] Processing: {string.Join(",", message.Data.AssetIds)}");

                    await HandleAsync(query, cancellationToken)
                        .Bind(async resp =>
                        {
                            return await _messageStreamManager
                            .ProduceResponseAsync(
                                correlationId,
                                replyTo,
                                resp, cancellationToken);
                        })
                        .MapErrors(errs => 
                        {
                            var task = _messageStreamManager
                                .ProduceErrorResponseAsync<GetAssetsByIdsResponse>(
                                    correlationId,
                                    replyTo,
                                    errs,
                                    cancellationToken);
                            Task.WaitAll(task);
                            return errs;
                        });

                    _logger.LogDebug($"✅ [Worker-{workerId}] Completed: {correlationId}");
                    
                    // Commit หลังจาก process เสร็จ
                    _consumer.Commit(consumerResult);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"❌ [Worker-{workerId}] Error processing {correlationId}: {ex.Message}");
                    
                    // no dead-queue
                    _consumer.Commit(consumerResult);
                }
            }

            _logger.LogDebug($"👷 Worker {workerId} finished");
        }

    }
}
