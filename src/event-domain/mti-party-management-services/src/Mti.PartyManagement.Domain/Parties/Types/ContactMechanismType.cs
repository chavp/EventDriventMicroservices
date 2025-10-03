using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties.Types
{
    [Table("ContactMechanismTypes")]
    [Index(nameof(Code), IsUnique = true)]
    public class ContactMechanismType : TypeModel
    {
        public const string Mobile = "MOBILE";
        public const string HomePhone = "HOME_PHONE";
        public const string OfficePhone = "OFFICE_PHONE";
        public const string Email = "EMAIL";

        public const string MaimAddress = "MAIN_ADDRESS";
        protected ContactMechanismType() { }
        public ContactMechanismType(string code)
        {
            Code = code;
        }
    }
}
