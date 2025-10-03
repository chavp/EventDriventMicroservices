using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Azure.Core;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Mti.Domain.Infrastructure.Common;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Mti.Domain.Infrastructure.Extensions
{
    public static class TopicExtensions
    {
        public static async Task CreateKafkaTopicAsync(
            string bootstrapServers, string topicName, 
            string? cleanupPolicy = null,
            int numPartitions = -1, short replicationFactor = -1)
        {
            var config = new AdminClientConfig { BootstrapServers = bootstrapServers };

            var congigDict = new Dictionary<string, string>
            {
                { "compression.type", "gzip" }
            };
            if (!string.IsNullOrEmpty(cleanupPolicy))
            {
                congigDict.Add("cleanup.policy", cleanupPolicy);
            }
            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                try
                {
                    var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                    var topicExists = metadata.Topics.Any(t => t.Topic == topicName);

                    if (!topicExists)
                    {
                        await adminClient.CreateTopicsAsync(new List<TopicSpecification>
                    {
                        new TopicSpecification
                        {
                            Name = topicName,
                            NumPartitions = numPartitions,
                            ReplicationFactor = replicationFactor,
                            Configs = congigDict
                        }
                    });
                        Console.WriteLine($"Topic '{topicName}' created successfully.");
                    }
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occurred creating topic '{topicName}': {e.Results[0].Error.Reason}");
                }
            }


        }
        public static async Task DeleteKafkaTopics(string brokerList, IEnumerable<string> topicNames)
        {
            var config = new AdminClientConfig { BootstrapServers = brokerList };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                try
                {
                    // DeleteTopicsAsync returns a Task that completes when the deletion is acknowledged by the broker.
                    await adminClient.DeleteTopicsAsync(topicNames);
                    Console.WriteLine($"Topics '{string.Join(", ", topicNames)}' deleted successfully.");
                }
                catch (CreateTopicsException ex)
                {
                    Console.WriteLine($"Error deleting topics: {ex.Results[0].Error.Reason}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                }
            }
        }

        public static async Task ProduceAsync<TKey, TValue>(
            ILogger logger,
            string bootstrapServers,
            string topic,
            TKey key,
            TValue message, string requestTopic,
            CancellationToken cancellationToken)
            where TValue : Message<TKey, TValue>
        {
            var msg = new Message<TKey, TValue>
            {
                Key = key,
                Value = message
            };
            var producerBuilder = new ProducerBuilder<TKey, TValue>(new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = Dns.GetHostName(),
                EnableIdempotence = true,
                Acks = Acks.All,
            })
            .SetErrorHandler((_, e) =>
            {
                logger.LogError($"Error in producer: {e.Reason}");
            })
            .SetValueSerializer(new JsonSerializer<TValue>())
            .SetKeySerializer(new JsonSerializer<TKey>());

            using(var producer = producerBuilder.Build())
            {
                var deliveryResult = await producer
                        .ProduceAsync(requestTopic, msg, cancellationToken);
                logger.LogDebug($"Message sent to partition {deliveryResult.Partition} with offset {deliveryResult.Offset}, key {key}");

            }
        }
    }
}
