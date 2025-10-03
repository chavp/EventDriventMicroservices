namespace Mti.PartyManagement.Messaging
{
    public sealed record SavePartiesByOrderResponse(Guid OrderId)
    {
        public string? Parties_TenantId { get; init; }
        public string? Orders_TenantId { get; init; }

        public IReadOnlyCollection<SavePartiesByOrderItemResponse> SaveRoleOrderItems { get; set; } = [];
    }

    public sealed record SavePartiesByOrderItemResponse(Guid OrderItemId)
    {
        public IReadOnlyCollection<PartyProfileResponse> Parties { get; set; } = [];
        public AssetResponse? Asset { get; set; }
    }

    public sealed record PartyProfileResponse(Guid PartyId, string PartyRoleTypeCode)
    {
        public IReadOnlyCollection<ContactMechanismResponse> ContactMechanisms { get; set; } = [];
    }

    public sealed record ContactMechanismResponse(Guid ContactMechanismId, string ContactMechanismTypeCode);
    public sealed record AssetResponse(Guid AssetId, string AssetRoleTypeCode);
}
