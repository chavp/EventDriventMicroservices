namespace Mti.Domain.Application.Abstractions.Messaging
{
    public interface IIntegrationEventConsumer
    {
        void Consume(IIntegrationEvent integrationEvent);
    }
}
