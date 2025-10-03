namespace Mti.ProductManagement.Messaging.Products.Commands
{
    public record SaveProductsByOrderRequest(Guid OrderId)
    {
        public string? OrderNumber { get; init; }
        public string? Orders_TenantId { get; init; }
        public IReadOnlyCollection<SaveProductByOrderItemRequest> OrderItems { get; set; } = [];
    }

    public record SaveProductByOrderItemRequest(Guid OrderItemId)
    {
        public uint? OrderItemSeq { get; init; }
        public ProductRequest? Product { get; set; }
        public CoverageRequest? Coverage { get; set; }
        public ProductFeatureRequest? ProductFeature { get; set; }
    }

    public record ProductRequest(string ProductCode, string ProductName); 
    public record CoverageRequest(string CoverageTypeCode,
        string CoverageLevelTypeCode,
        string CoverageLevelBasisCode,
        decimal? Amount,
        decimal? Percentage,
        decimal? LimitFrom,
        decimal? LimitTo);
    public sealed record ProductFeatureRequest(
        string ProductFeatureTypeCode,
        string ProductFeatureCode,
        string ProductFeatureName);
}
