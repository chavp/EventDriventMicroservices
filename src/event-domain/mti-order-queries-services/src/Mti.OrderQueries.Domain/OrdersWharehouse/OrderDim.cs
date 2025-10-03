using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.OrderQueries.Domain.OrdersWharehouse
{
    [Index(nameof(OrderId), IsUnique = true)]
    [Index(nameof(OrderSaleDate))]
    [Index(nameof(OrderLoanNumber))]
    [Index(nameof(OrderSaleDate), nameof(OrderLoanNumber))]
    public class OrderDim: StarModel
    {
        [StringLength(200)]
        public string? Orders_TenantId { get; set; }

        [Required]
        public Guid? OrderId { get; set; }
        [Required]
        public string? OrderNumber { get; set; }
        [Required]
        public DateOnly? OrderSaleDate { get; set; }

        [Required, StringLength(50)]
        public string? OrderLoanNumber { get; set; }

    }
}
