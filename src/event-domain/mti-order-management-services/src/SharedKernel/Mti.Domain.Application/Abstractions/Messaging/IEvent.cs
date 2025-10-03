using MediatR;

namespace Mti.Domain.Application.Abstractions.Messaging
{
    /// <summary>
    /// Represents the event interface.
    /// </summary>
    public interface IEvent : INotification
    {
    }
}