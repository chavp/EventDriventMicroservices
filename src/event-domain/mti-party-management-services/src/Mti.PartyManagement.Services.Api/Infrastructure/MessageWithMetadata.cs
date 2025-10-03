using Confluent.Kafka;
using Mti.PartyManagement.Messaging.Parties.GetAssetsByIds;

namespace Mti.PartyManagement.Services.Api.Infrastructure
{
    public class MessageWithMetadata<TData>
    {
        public string? CorrelationId { get; set; }
        public string? ReplyTo { get; set; }
        public TData? Data { get; set; }
        public ConsumeResult<string, TData>? ConsumeResult { get; set; }
        public DateTime ReceivedAt { get; set; } = DateTime.UtcNow;
    }
}
