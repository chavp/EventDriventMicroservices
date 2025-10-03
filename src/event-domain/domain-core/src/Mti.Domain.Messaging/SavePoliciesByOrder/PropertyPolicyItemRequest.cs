namespace Mti.Domain.Messaging.SavePoliciesByOrder
{
    public sealed record PropertyPolicyItemRequest(string AgreementText) 
        : AgreementItemRequest(AgreementText)
    {
        public Guid? Parties_AssetId { get; init; }
    }
}
