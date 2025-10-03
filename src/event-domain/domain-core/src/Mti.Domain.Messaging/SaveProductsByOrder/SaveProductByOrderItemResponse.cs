namespace Mti.Domain.Messaging.SaveProductByOrder
{
    public sealed record SaveProductByOrderItemResponse(
       Guid ProductId,
       Guid OrderItemId
    )
    {
        public IReadOnlyCollection<SaveCoverageByOrderResponse> Coverages { get; set; } = [];
        public IReadOnlyCollection<SaveProductFeatureByOrderResponse> ProductFeatures { get; set; } = [];
    }
}
