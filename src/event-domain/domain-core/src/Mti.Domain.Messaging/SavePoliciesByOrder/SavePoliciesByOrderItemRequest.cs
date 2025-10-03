namespace Mti.Domain.Messaging.SavePoliciesByOrder
{
    public sealed record SavePoliciesByOrderItemRequest(
        Guid OrderItemId,
        string AgreementTypeCode,
        string PeriodTypeCode,
        string PolicyNumber)
    {
        public string? PolicyPreviousNumber { get; init; }
        public decimal Premium { get; init; }
        public DateOnly? PolicyEffectiveDate { get; init; }
        public DateOnly? PolicyExpiryDate { get; init; }

        public IReadOnlyCollection<AgreementItemRequest> AgreementItems { get; set; } = [];
    }
}
