using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;

namespace Mti.Domain.Infrastructure.Common
{
    public class AutoDeleteReplyTopicService
    {
        private readonly IAdminClient _adminClient;
        private readonly string _topicPrefix = "reply_temp";
        private readonly string _env = "dev";
        private readonly ILogger _logger;
        public AutoDeleteReplyTopicService(
            ILogger logger,
            IAdminClient adminClient, 
            string env)
        {
            _logger = logger;
            _adminClient = adminClient;
            _env = env;
        }

        public async Task<string> CreateTemporaryReplyTopic(string correlationId)
        {
            var topicName = $"{_env}.{_topicPrefix}.{correlationId}.{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";

            var topicSpec = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 1,
                ReplicationFactor = 1,
                Configs = new Dictionary<string, string>
                {
                    // ลบอัตโนมัติหลัง 10 นาที
                    { "retention.ms", "600000" },
                    // ลบ segment ทันทีเมื่อหมดอายุ
                    { "segment.ms", "60000" },
                    // ตรวจสอบการลบทุก 30 วินาที
                    { "delete.retention.ms", "30000" }
                }
            };

            await _adminClient.CreateTopicsAsync(new[] { topicSpec });

            // รอให้ topic พร้อม
            await Task.Delay(2000);

            // ตั้ง timer ลบ topic (backup plan)
            _ = Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromMinutes(15)); // รอเพิ่มเติม
                await DeleteTopicSafely(topicName);
            });

            return topicName;
        }

        private async Task DeleteTopicSafely(string topicName)
        {
            try
            {
                await _adminClient.DeleteTopicsAsync(new[] { topicName });
            }
            catch (DeleteTopicsException ex)
            {
                // Log แต่ไม่ throw เพราะ topic อาจถูกลบไปแล้ว
                _logger.LogError($"Could not delete topic {topicName}: {ex.Message}");
            }
        }
    }
}
