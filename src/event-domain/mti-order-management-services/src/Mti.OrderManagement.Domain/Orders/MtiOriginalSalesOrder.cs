using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace Mti.OrderManagement.Domain.Orders
{
    [Table("MtiOriginalSalesOrders")]
    [Index(nameof(SaleDate), nameof(LoanNumber), IsUnique = true)]
    public sealed class MtiOriginalSalesOrder : SalesOrder
    {
        internal MtiOriginalSalesOrder(string orderNumber,
            DateOnly? saleDate,
            string? loanNumber) : base(orderNumber) 
        {
            SaleDate = saleDate;
            LoanNumber = loanNumber;
        }

        [Required]
        public DateOnly? SaleDate { get; set; }

        [Required, StringLength(50)]
        public string? LoanNumber { get; set; }

        public MtiOriginalSalesOrderItem CreateItem(string status,
            string? productName, string? policyType,
            string? campaign, string? package, string? workshop)
        {
            var item = new MtiOriginalSalesOrderItem(
                status,
                productName,
                policyType,
                campaign,
                package,
                workshop);

            return item;
        }
    }
}
