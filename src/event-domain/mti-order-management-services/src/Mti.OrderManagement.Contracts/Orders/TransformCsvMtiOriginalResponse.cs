using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record TransformCsvMtiOriginalResponse
    {
        public int Total { get; set; }
        public int Page { get; set; }
        public int Limit { get; set; }
        public ReadOnlyCollection<TranformMtiOriginalRequest> Data { get; set; }
    }
}
