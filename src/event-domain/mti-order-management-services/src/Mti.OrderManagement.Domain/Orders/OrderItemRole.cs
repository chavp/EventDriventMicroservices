using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Mti.Domain.Core.Primitives;
using Mti.OrderManagement.Domain.Orders.Types;

namespace Mti.OrderManagement.Domain.Orders
{
    [Table("OrderItemRoles")]
    [Index(nameof(Seq), nameof(OrderItemId), nameof(OrderRoleTypeId), nameof(Parties_PartyId), IsUnique = true
        )
        ]
    public class OrderItemRole : EntityAuditable
    {
        protected OrderItemRole() { }
        public OrderItemRole(Guid? orderItemId,
            Guid? orderRoleTypeId,
            Guid? partiesPartyId)
        {
            OrderItemId = orderItemId;
            OrderRoleTypeId = orderRoleTypeId;
            Parties_PartyId = partiesPartyId;
        }

        public ushort Seq { get; set; }

        [Required]
        public Guid? OrderItemId { get; set; }
        public OrderItem? OrderItem { get; set; }

        [Required]
        public Guid? OrderRoleTypeId { get; set; }
        public OrderRoleType? OrderRoleType { get; set; }

        [Required]
        public Guid? Parties_PartyId { get; set; }
    }
}
