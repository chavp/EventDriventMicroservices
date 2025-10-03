using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.OrderQueries.Domain.OrdersWharehouse
{
    [Index(nameof(OrderItemId), IsUnique = true)]
    public class OrderItemFact : StarModel
    {

        [Required]
        public Guid? OrderItemId { get; set; }

        public uint OrderItemSeq { get; set; }

        public decimal OrderItemPrice { get; set; }
        public uint OrderItemQuantity { get; set; }

        public Guid? OrderItemOrderedForId { get; set; }

        [Required]
        public Guid? OrderDimKey { get; set; }

        [ForeignKey(nameof(OrderDimKey))]
        public OrderDim? OrderDim { get; set; }

        public Guid? ApplicationDimKey { get; set; }
        [ForeignKey(nameof(ApplicationDimKey))]
        public ApplicationDim? ApplicationDim { get; set; }

        public Guid? ProductDimKey { get; set; }
        [ForeignKey(nameof(ProductDimKey))]
        public ProductDim? ProductDim { get; set; }

        public Guid? InsuredAssetDimKey { get; set; }
        [ForeignKey(nameof(InsuredAssetDimKey))]
        public InsuredAssetDim? InsuredAssetDim { get; set; }

        public List<OrderItemPartyRoleDim> OrderItemPartyRoleDims { get; set; } = [];
    }
}
