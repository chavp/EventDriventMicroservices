using System.Text;
using Confluent.Kafka;
using Mti.Domain.Application.Abstractions.Messaging;

namespace Mti.OrderManagement.Infrastructure.Streaming
{
    public class IntegrationEventConsumer : IIntegrationEventConsumer
    {
        readonly IConsumer<Ignore, byte[]> _consumer = null;

        public void Consume(IIntegrationEvent integrationEvent)
        {
            CancellationTokenSource cts = new CancellationTokenSource();

            while (true)
            {
                try
                {
                    var cr = _consumer.Consume(cts.Token);

                    var msg = Encoding.UTF8.GetString(cr.Value);
                    
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}");
                }
            }
        }
    }
}
