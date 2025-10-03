using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Azure.Core;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Mti.Domain.Infrastructure.Common
{
    public class MessageStreamBuilder
    {
        public string BootstrapServers {  get; private set; }

        private readonly ILogger _logger;
        public MessageStreamBuilder(ILogger logger)
        {
            _logger = logger;
        }

        public MessageStreamBuilder WithBootstrapServers(string bootstrapServers)
        {
            BootstrapServers = bootstrapServers;
            return this;
        }

        public IConsumer<string, TValue> BuildReplyCunsumer<TValue>()
        {
            var groupId = $"reply.{Guid.NewGuid()}";
            var consumerBuilder = new ConsumerBuilder<string, TValue>(new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SessionTimeoutMs = 30000,
                MaxPollIntervalMs = 300000,
                // สำคัญ: รอให้ metadata โหลดเสร็จ
                TopicMetadataRefreshIntervalMs = 5000
            })
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError($"Error in consumer: {e.Reason}");
            })
            .SetValueDeserializer(new JsonDeserializer<TValue>())
            ;
            return consumerBuilder.Build();
        }
        public IProducer<string, TValue> BuildProducer<TValue>()
        {
            var producer = new ProducerBuilder<string, TValue>(new ProducerConfig
            {
                BootstrapServers = BootstrapServers,
                ClientId = Dns.GetHostName(),
                EnableIdempotence = true,
                Acks = Acks.All,
            })
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError($"Error in producer: {e.Reason}");
            })
            .SetValueSerializer(new JsonSerializer<TValue>())
            .Build();
            return producer;
        }

        public Message<string, TValue> BuildCorrelationMessage<TValue>(TValue request, string correlationId, string replyTo)
        {
            var message = new Message<string, TValue>
            {
                Key = correlationId.ToString(),
                Value = request,
                Headers = new Headers
                {
                    { "CorrelationId", Encoding.UTF8.GetBytes(correlationId.ToString()) },
                    { "ReplyTo", Encoding.UTF8.GetBytes(replyTo) },
                }
            };
            return message;
        }
    }
}
