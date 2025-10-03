namespace Mti.PartyManagement.Messaging
{
    public sealed record SavePartiesByOrderItemRequest(Guid OrderItemId)
    {
        public uint? OrderItemSeq { get; init; }
        public IReadOnlyCollection<PartyProfileRequest> Parties { get; set; } = [];
        public AssetRequest? InsuredAsset { get; set; }
    }
}
