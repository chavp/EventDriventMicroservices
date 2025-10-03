using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.PartyManagement.Messaging.Parties
{
    public record PostalAddressMessage
    {
        public Guid? ContactMechanismId { get; set; }
        public string? ContactMechanismTypeCode { get; set; }
        public string? Name { get; set; }
        public string? HouseNumber { get; set; }
        public string? VillageNumber { get; set; }
        public string? Village { get; set; }
        public string? Alley { get; set; }
        public string? Road { get; set; }
        public string? Building { get; set; }
        public string? Room { get; set; }
        public string? Floor { get; set; }
        public string? Province { get; set; }
        public string? District { get; set; }
        public string? SubDistrict { get; set; }
        public string? ZipCode { get; set; }
        public virtual string? DisplayName { get; set; }
    }
}
