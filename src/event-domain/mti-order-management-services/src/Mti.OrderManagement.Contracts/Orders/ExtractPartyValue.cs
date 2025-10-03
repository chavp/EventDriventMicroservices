using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Contracts.Orders.Enums;
using Mti.OrderManagement.Messaging;
using Mti.PartyManagement.Messaging.Parties;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record ExtractPartyValue
        : PartyMessage
    {
        public EnumPatternNames Pattern { get; set; }

        public new List<ExtractPostalAddressValue> PostalAddresses { get; set; } = [];
    }
}
