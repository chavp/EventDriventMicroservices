namespace Mti.ProductManagement.Messaging.Products.Commands
{
    public record SaveProductsByOrderResponse(Guid OrderId)
    {
        public string? Orders_TenantId { get; init; }
        public string? Products_TenantId { get; init; }

        public IReadOnlyCollection<SaveProductsByOrderItemResponse> OrderItems { get; set; } = [];
    }

    public record SaveProductsByOrderItemResponse(Guid OrderItemId)
    {
        public ProductResponse? Product { get; set; }
        public CoverageResponse? Coverage { get; set; }
        public ProductFeatureResponse? ProductFeature { get; set; }
    }

    public record ProductResponse(Guid ProductId);
    public record CoverageResponse(Guid CoverageTypeId, Guid CoverageLevelId);
    public record ProductFeatureResponse(Guid ProductFeatureId);

}
