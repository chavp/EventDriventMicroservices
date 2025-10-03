using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Mti.Domain.Core.Primitives;

namespace Mti.OrderManagement.Domain.Orders
{
    [Table("OrderItems")]
    [Index(nameof(OrderId), nameof(Seq),
        nameof(Products_ProductId),
        nameof(Products_ProductFeatureId),
        nameof(Products_CoverageTypeId),
        nameof(Products_CoverageLevelId),
        IsUnique = true)]
    public abstract class OrderItem : EntityAuditable
    {
        public decimal Price { get; set; }
        public int Quantity { get; set; }

        [Required]
        public Guid? OrderId { get; set; }
        public Order? Order { get; set; }

        public uint Seq { get; set; }

        public Guid? Products_ProductId { get; set; }
        public Guid? Products_ProductFeatureId { get; set; }
        public Guid? Products_CoverageTypeId { get; set; }
        public Guid? Products_CoverageLevelId { get; set; }

        public Guid? OrderedForId { get; set; }
        public OrderItem? OrderedFor { get; set; }

        public List<OrderItemRole> Roles { get; set; } = [];

        public List<PoliciesAgreementItem> AgreementItems { get; set; } = [];
    }
}
