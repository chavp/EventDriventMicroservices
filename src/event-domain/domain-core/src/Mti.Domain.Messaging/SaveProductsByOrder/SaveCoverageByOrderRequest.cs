namespace Mti.Domain.Messaging.SaveProductByOrder
{
    public sealed record SaveCoverageByOrderRequest(
        string CoverageTypeCode,
        string CoverageLevelTypeCode,
        string CoverageLevelBasisCode,
        decimal Amount = 0,
        decimal Percentage = 0,
        decimal LimitFrom = 0, 
        decimal LimitTo = 0);

    
}
