namespace Mti.Domain.Messaging.SavePartiesByOrder
{
    public record ContactMechanismRequest(string ContactMechanismTypeCode)
    {
        public TelecommunicationRequest? Telecommunication { get; init; }
        public ElectronicAddressRequest? ElectronicAddress { get; init; }
        public PostalAddressRequest? PostalAddress { get; init; }
    }
}
