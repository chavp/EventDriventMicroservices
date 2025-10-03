namespace Mti.Domain.Messaging.SaveProductByOrder
{
    public sealed record SaveCoverageByOrderResponse(
        Guid CoverageTypeId,
        Guid CoverageLevelTypeId);
}
