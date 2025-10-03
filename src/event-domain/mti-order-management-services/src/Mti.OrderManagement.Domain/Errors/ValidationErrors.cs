using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Primitives;

namespace Mti.OrderManagement.Domain.Errors
{
    public static class ValidationErrors
    {
        public static class TransformMtiOriginal
        {
            public static Error LoanNumberIsRequired => new Error($"{nameof(TransformMtiOriginal)}.{nameof(LoanNumberIsRequired)}", "The LoanNumber is required.");
            public static Error SaleDateIsRequired => new Error($"{nameof(TransformMtiOriginal)}.{nameof(SaleDateIsRequired)}", "The SaleDate is required.");
            public static Error ItemsIsRequired => new Error($"{nameof(TransformMtiOriginal)}.{nameof(ItemsIsRequired)}", "The Items is required.");
        }
    }
}
