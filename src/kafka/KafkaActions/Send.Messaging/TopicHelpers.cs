using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Send.Messaging
{
    public static class TopicHelpers
    {
        public static async Task CreateKafkaTopicAsync(string bootstrapServers, string topicName, int numPartitions = 1, short replicationFactor = 1)
        {
            var config = new AdminClientConfig { BootstrapServers = bootstrapServers };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new List<TopicSpecification>
                    {
                        new TopicSpecification
                        {
                            Name = topicName,
                            NumPartitions = numPartitions,
                            ReplicationFactor = replicationFactor,
                            Configs = new Dictionary<string, string>
                            {
                                { "cleanup.policy", "compact" }, // Example configuration
                                { "compression.type", "lz4" } // Example configuration
                            }
                        }
                    });
                    Console.WriteLine($"Topic '{topicName}' created successfully.");
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
    }
}
