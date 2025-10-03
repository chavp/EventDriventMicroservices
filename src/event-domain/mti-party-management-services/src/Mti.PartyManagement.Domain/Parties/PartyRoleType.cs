using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("PartyRoleTypes")]
    [Index(nameof(Code), IsUnique = true)]
    public class PartyRoleType : TypeModel
    {
        public const string Insured = "INSURED";
        public const string Invoice = "INVOICE";

        protected PartyRoleType() { }
        public PartyRoleType(string code)
        {
            Code = code;
        }
    }
}
