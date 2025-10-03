using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties.Types
{
    [Table("PartyTitles")]
    [Index(nameof(Code), IsUnique = true)]
    public class PartyTitle : TypeModel
    {
        protected PartyTitle() { }
        public PartyTitle(string code)
        {
            Code = code;
        }

        public bool IsOrganization { get; set; }
    }
}
