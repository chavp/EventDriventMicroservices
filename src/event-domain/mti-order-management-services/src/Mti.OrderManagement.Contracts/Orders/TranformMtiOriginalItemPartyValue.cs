using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record TranformMtiOriginalItemPartyValue
    {
        public PartyNameValue? Name { get; set; }

        public string? CardId { get; set; }

        public DateOnly? BirthDate { get; set; }
        public string? Nationlity { get; set; }

        // contact 
        public string? TelMobile1 { get; set; }
        public string? TelMobile { get; set; }
        public string? TelHome { get; set; }
        public string? TelOffice { get; set; }
        public string? Email { get; set; }

        public string? BranchId { get; set; }
        public string? BranchName { get; set; }

        // address
        public AddressValue? Addr { get; set; }
    }

    public sealed class PartyNameValue
    {
        public string? TitleText { get; set; }
        public string? Givenname { get; set; }
        public string? Surname { get; set; }
        public string? Fullname { get; set; }
    }

    public sealed class AddressValue
    {
        public string? No { get; set; }
        public string? Moo { get; set; }
        public string? Floor { get; set; }
        public string? Room { get; set; }
        public string? Mooban { get; set; }
        public string? Building { get; set; }
        public string? Soi { get; set; }
        public string? Road { get; set; }
        public string? Tumbol { get; set; }
        public string? Ampur { get; set; }
        public string? Province { get; set; }
        public string? Zipcode { get; set; }

        public string? Line1 { get; set; }
        public string? Line2 { get; set; }
        public string? Line3 { get; set; }
        public string? Line4 { get; set; }
    }
}
