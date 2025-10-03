using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Domain.Orders
{
    [Table("SalesOrders")]
    public class SalesOrder : Order
    {
        internal SalesOrder(string orderNumber) : base(orderNumber) { }
    }
}
