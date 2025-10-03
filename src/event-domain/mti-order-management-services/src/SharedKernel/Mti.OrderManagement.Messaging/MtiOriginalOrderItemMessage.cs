using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.PartyManagement.Messaging.Parties;

namespace Mti.OrderManagement.Messaging
{
    public record MtiOriginalOrderItemMessage
    {
        public string? OrderItemTypeCode { get; set; }
        public uint Seq { get; set; }
        public Guid? OrderItemId { get; set; }
        public decimal NetPremium { get; set; }
        public uint Quantity { get; set; }

        public virtual MtiOriginalApplicationMessage? Application { get; set; }
        public virtual MtiOriginalProductMessage? Product { get; set; }
        public virtual MtiOriginalCoverageMessage? Coverage { get; set; }

        public virtual PolicyMessage? Policy { get; set; }
        public virtual AssetMessage? InsuredAsset { get; set; }
        public virtual IReadOnlyList<PartyMessage> Parties { get; set; } = [];
    }
}
