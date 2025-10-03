namespace Mti.Domain.Messaging.SaveProductByOrder
{
    public sealed record SaveProductFeatureByOrderRequest(
        string ProductFeatureTypeCode,
        string ProductFeatureCode,
        string ProductFeatureName);

    
}
