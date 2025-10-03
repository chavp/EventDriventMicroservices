using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.Domain.Messaging.SaveProductByOrder
{
    public sealed record SaveProductsByOrderResponse(Guid OrderId)
    {
        public Guid? Products_TenantId { get; init; }
        public Guid? Orders_TenantId { get; init; }

        public IReadOnlyCollection<SaveProductByOrderItemResponse> OrderItems { get; set; } = [];
    }
}
