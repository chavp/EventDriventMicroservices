using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.Domain.Messaging.SavePoliciesByOrder
{
    public sealed record SavePoliciesByOrderRequest(Guid OrderId, string OrderNumber)
    {
        public Guid? Orders_TenantId { get; init; }
        public Guid? Products_TenantId { get; init; }
        public Guid? Parties_TenantId { get; init; }
        public Guid? Products_ProductId { get; init; }

        public IReadOnlyCollection<DeleteAgreementItemRequest> DeleteAgreementItems { get; set; } = [];
        public IReadOnlyCollection<SavePoliciesByOrderItemRequest> OrderItems { get; set; } = [];
    }
}
