using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.PartyManagement.Messaging.Parties
{
    public record AssetMessage(string AssetTypeCode)
    {
        public Guid? AssetId { get; set; }
        public VehicleMessage? Vehicle { get; set; }
    }
}
