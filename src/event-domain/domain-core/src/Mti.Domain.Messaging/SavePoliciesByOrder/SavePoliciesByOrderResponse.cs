using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.Domain.Messaging.SavePoliciesByOrder
{
    public sealed record SavePoliciesByOrderResponse(Guid OrderId)
    {
        public Guid? Policies_TenantId { get; init; }
        public Guid? Orders_TenantId { get; init; }

        public IReadOnlyCollection<SavePoliciesByOrderItemResponse> OrderItems { get; set; } = [];
    }
}
