using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Mti.Domain.Core.Primitives;

namespace Mti.OrderManagement.Domain.Orders.Types
{
    [Table("OrderRoleTypes")]
    [Index(nameof(Code), IsUnique = true)]
    public sealed class OrderRoleType : TypeModel
    {
        public const string Insured = "INSURED";
        public const string Invoice = "INVOICE";

        public const string Owner = "OWNER";

        protected OrderRoleType() { }
        public OrderRoleType(string code)
        {
            Code = code;
        }
    }
}
