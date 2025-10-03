using System.Collections.ObjectModel;

namespace Mti.OrderManagement.Messaging
{
    public record class MtiOriginalOrderMessage 
    {
        public string? Orders_TenantId { get; set; }
        public string? Parties_TenantId { get; set; }
        public string? Products_TenantId { get; set; }

        public string? OrderTypeCode { get; set; }
        public Guid? OrderId { get; set; }
        public string? Number { get; set; }
        public DateOnly? SaleDate { get; set; }
        public string? LoanNumber { get; set; }
        public long TotalQuantity { get; set; }

        public virtual IReadOnlyList<MtiOriginalOrderItemMessage> OrderItems { get; set; } = [];

    }
}
