using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderQueries.Domain.OrdersWharehouse
{
    public class OrderItemPartyRoleDim : StarModel
    {
        [Required]
        public Guid? OrderItemFactKey { get; set; }
        [ForeignKey(nameof(OrderItemFactKey))]
        public OrderItemFact? OrderItemFact { get; set; }

        [Required]
        public Guid? PartyDimKey { get; set; }
        [ForeignKey(nameof(PartyDimKey))]
        public PartyDim? PartyDim { get; set; }

        [StringLength(256)]
        public string? OrderItemRoleTypeCode { get; set; }

        public uint OrderItemRoleSeq { get; set; }
    }
}
