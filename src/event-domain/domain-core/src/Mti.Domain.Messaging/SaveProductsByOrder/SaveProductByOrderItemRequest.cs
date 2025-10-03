namespace Mti.Domain.Messaging.SaveProductByOrder
{
    public sealed record SaveProductByOrderItemRequest(
        Guid OrderItemId,
        string? ProductName,
        string? ProductCode
    )
    {
        public IReadOnlyCollection<SaveCoverageByOrderRequest> Coverages { get; set; } = [];
        public IReadOnlyCollection<SaveProductFeatureByOrderRequest> ProductFeatures { get; set; } = [];
    }

    
}
