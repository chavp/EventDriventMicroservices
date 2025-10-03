using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using Mti.Domain.Core.Primitives;

namespace Mti.OrderManagement.Domain.Orders
{
    [Index(nameof(OrderNumber), IsUnique = true)]
    public abstract class Order : EntityAggregateRoot
    {
        protected Order() { }
        public Order(string ? orderNumber)
        {
            OrderNumber = orderNumber;
        }

        [Required, StringLength(200)]
        public string? OrderNumber { get; set; }
        public DateOnly? OrderDate { get; set; }
        public decimal TotalAmount { get; set; }
        public int TotalQuantity { get; set; }
        public List<OrderItem> Items { get; set; } = [];
        public void AddItem(OrderItem item)
        {
            if (Items.Any())
            {
                item.Seq = Items.Max(x => x.Seq) + 1;
            }
            Items.Add(item);
            TotalAmount += item.Price * item.Quantity;
            TotalQuantity = Items.Sum(x => x.Quantity);
        }

        // external Domain
        [StringLength(200)]
        public string? Products_TenantId { get; set; }

        [StringLength(200)]
        public string? Parties_TenantId { get; set; }

        [StringLength(200)]
        public string? Policies_TenantId { get; set; }

        public static SalesOrder CreateSalesOrder(string orderNumber)
        {
            var newOrder = new SalesOrder(orderNumber);
            return newOrder;
        }

        public static MtiOriginalSalesOrder CreateMtiOriginalSalesOrder(
            string? orderNumber, DateOnly? saleDate, string? loanNumber)
        {
            var newOrder = new MtiOriginalSalesOrder(orderNumber, saleDate, loanNumber);
            newOrder.OrderDate = saleDate;

            return newOrder;
        }
    }
}
