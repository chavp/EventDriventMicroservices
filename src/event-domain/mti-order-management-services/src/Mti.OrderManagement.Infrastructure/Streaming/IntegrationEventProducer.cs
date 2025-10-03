using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Mti.Domain.Application.Abstractions.Messaging;
using Mti.OrderManagement.Infrastructure.Streaming.Settings;
using Newtonsoft.Json;

namespace Mti.OrderManagement.Infrastructure.Streaming
{
    public sealed class IntegrationEventProducer : IIntegrationEventPublisher, IDisposable
    {
        private readonly StreamBrokerSettings _streamBrokerSettings;
        private readonly IProducer<Null, byte[]> _producer = null;

        public IntegrationEventProducer(IOptions<StreamBrokerSettings> streamBrokerSettings)
        {
            _streamBrokerSettings = streamBrokerSettings.Value;

            _producer = new ProducerBuilder<Null, byte[]>(new ProducerConfig
            {
                BootstrapServers = _streamBrokerSettings.BootstrapServers,

            }).Build();
        }

        public void Publish(IIntegrationEvent integrationEvent)
        {
            string payload = JsonConvert.SerializeObject(integrationEvent, typeof(IIntegrationEvent), new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto
            });

            byte[] body = Encoding.UTF8.GetBytes(payload);

            _producer.Produce(_streamBrokerSettings.Topic, new Message<Null, byte[]> { Value = body },
                deliveryHandler: dr =>
                {
                    
                });
        }

        public void Dispose()
        {
            _producer.Dispose();
        }

    }
}
