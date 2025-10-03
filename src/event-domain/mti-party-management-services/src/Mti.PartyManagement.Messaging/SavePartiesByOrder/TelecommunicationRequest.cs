namespace Mti.PartyManagement.Messaging
{
    public sealed record TelecommunicationRequest(string Number) 
    {
        public string? AreaCode { get; init; }
        public string? ContryCode { get; init; }
    }
}
