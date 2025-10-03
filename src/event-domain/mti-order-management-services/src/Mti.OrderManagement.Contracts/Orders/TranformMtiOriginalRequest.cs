using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record TranformMtiOriginalRequest
    {
        public DateOnly? SaleDate { get; set; }
        public string? LoanNumber { get; set; }

        public IReadOnlyList<TranformMtiOriginalItemRequest> Items { get; set; } = [];
    }
}
