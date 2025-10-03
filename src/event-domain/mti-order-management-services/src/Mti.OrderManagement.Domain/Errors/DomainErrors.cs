using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Primitives;

namespace Mti.OrderManagement.Domain.Errors
{
    public static class DomainErrors
    {
        public static class Order
        {
            public static Error NotFound => new Error("Order.NotFound", "The user with the specified identifier was not found.");
            internal static Error OrderItemsIsRequired => new Error(
                "UpdatePersonalEvent.OrderItemsIsRequired",
                "The group event identifier is required.");
        }
    }
}
