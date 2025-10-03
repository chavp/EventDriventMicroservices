namespace Mti.Domain.Messaging.SavePartiesByOrder
{
    public sealed record PartyProfileRequest(string PartyRoleTypeCode)
    {
        public bool IsOrganization { get; init; }
        public string? Title { get; init; }
        public string? FirstName { get; init; }
        public string? LastName { get; init; }
        public string? MiddleName { get; init; }
        public string? CardId { get; init; }
        public string? Nationality { get; init; }
        public DateOnly? BirthDate { get; init; }

        public IReadOnlyCollection<ContactMechanismRequest> ContactMechanisms { get; set; } = [];

    }
}
