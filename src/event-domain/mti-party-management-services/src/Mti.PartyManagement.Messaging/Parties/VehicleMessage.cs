using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.PartyManagement.Messaging.Parties
{
    public record VehicleMessage
    {
        public string? Code { get; set; }
        public string? Brand { get; set; }
        public string? Model { get; set; }
        public ushort? ManufactoringYear { get; set; }
        public string? Color { get; set; }
        public string? RegisterNo { get; set; }
        public string? RegisterProvince { get; set; }
        public ushort? RegisterYear { get; set; }
        public string? Chassis { get; set; }
        public float? Cc { get; set; }
        public float? Seat { get; set; }
        public float? Weight { get; set; }
        public float? Tonnage { get; set; }
        public string? Engine { get; set; }
        public ushort? Passenger { get; set; }
    }
}
