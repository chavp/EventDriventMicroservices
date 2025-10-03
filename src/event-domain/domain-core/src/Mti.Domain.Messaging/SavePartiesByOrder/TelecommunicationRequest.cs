namespace Mti.Domain.Messaging.SavePartiesByOrder
{
    public sealed record TelecommunicationRequest(string Number) 
    {
        public string? AreaCode { get; init; }
        public string? ContryCode { get; init; }
    }
}
