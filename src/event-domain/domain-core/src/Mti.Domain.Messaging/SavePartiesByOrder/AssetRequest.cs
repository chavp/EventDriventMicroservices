namespace Mti.Domain.Messaging.SavePartiesByOrder
{
    public record AssetRequest(
        string AssetTypeCode,
        string AssetRoleTypeCode,
        string Name)
    {
        public VehicleAssetRequest? Vehicle { get; init; }
    }
}
