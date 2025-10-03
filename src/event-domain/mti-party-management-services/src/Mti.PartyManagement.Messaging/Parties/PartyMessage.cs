using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.PartyManagement.Messaging.Parties
{
    public record PartyMessage
    {
        public string PartyTypeCode
        {
            get
            {
                if (IsOrganization.HasValue)
                {
                    return IsOrganization.Value ? "ORGANIZATION" : "PERSON";
                }
                return "UNDEFINED";
            }
        }
        public Guid? PartyId { get; set; }
        public string? RoleTypeCode { get; set; }
        public uint ID { get; set; }
        public bool? IsOrganization { get; set; }
        public string? TitleName { get; set; }
        public string? FirstName { get; set; }
        public string? MiddleName { get; set; }
        public string? LastName { get; set; }

        public string? CardId { get; set; }
        public string? Nationality { get; set; }
        public DateOnly? BirthDate { get; set; }

        public virtual List<PostalAddressMessage> PostalAddresses { get; set; } = [];
    }
}
