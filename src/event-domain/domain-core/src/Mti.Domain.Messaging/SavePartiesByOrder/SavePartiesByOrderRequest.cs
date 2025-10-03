using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.Domain.Messaging.SavePartiesByOrder
{
    public sealed record SavePartiesByOrderRequest(Guid OrderId)
    {
        public Guid? Orders_TenantId { get; init; }
        public string? OrderNumber { get; init; }
        public IReadOnlyCollection<SavePartiesByOrderItemRequest> SaveRoleOrderItems { get; set; } = [];
        public IReadOnlyCollection<DeletePartyRequest> DeleteParties { get; set; } = [];
        public IReadOnlyCollection<DeleteAssetRequest> DeleteAssets { get; set; } = [];
    }
}
