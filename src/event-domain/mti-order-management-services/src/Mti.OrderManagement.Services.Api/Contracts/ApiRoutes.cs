namespace Mti.OrderManagement.Services.Api.Contracts
{
    /// <summary>
    /// Contains the API endpoint routes.
    /// </summary>
    public static class ApiRoutes
    {
        public static class OrderManagement
        {
            public const string TransformMtiOriginal = "order-management/transform-mti-original/sale-order";
            public const string TransformCsvMtiOriginal = "order-management/transform-csv-mti-original/sale-order";

            public const string SaveMtiOriginal = "order-management/save-mti-original/sale-order";

            public const string GetMtiOriginalOrderById = "order-management/orders/{orderId}/mti-original";

        }
    }
}
