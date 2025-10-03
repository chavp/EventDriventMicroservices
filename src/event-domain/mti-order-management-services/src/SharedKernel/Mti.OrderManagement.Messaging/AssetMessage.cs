using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.PartyManagement.Messaging.Parties;

namespace Mti.OrderManagement.Messaging
{
    public record AssetMessage
    {
        public Guid? AssetId { get; set; }
        public Guid? InsuredAssetId { get; set; }
        public string? AssetTypeCode { get; set; }
        public string? Description { get; set; }

        public virtual VehicleMessage? Vehicle { get; set; }
    }
}
