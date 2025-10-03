using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.Domain.Messaging.SaveProductByOrder
{
    public sealed record SaveProductsByOrderRequest(Guid OrderId)
    {
        public Guid? Orders_TenantId { get; init; }
        public IReadOnlyCollection<SaveProductByOrderItemRequest> OrderItems { get; set; } = [];
    };    
}
