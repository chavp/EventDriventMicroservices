using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Primitives;

namespace Mti.OrderManagement.Domain.Orders
{
    [Table("PoliciesAgreementItems")]
    public class PoliciesAgreementItem : EntityAuditable
    {
        [Required]
        public Guid? OrderItemId { get; set; }
        public OrderItem? OrderItem { get; set; }


        public Guid? Policies_AgreementItemId { get; set; }
    }
}
