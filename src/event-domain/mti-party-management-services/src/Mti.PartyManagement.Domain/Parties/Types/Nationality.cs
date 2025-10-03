using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties.Types
{
    [Table("Nationalities")]
    [Index(nameof(Code), IsUnique = true)]
    public class Nationality : TypeModel
    {
        protected Nationality() { }
        public Nationality(string code)
        {
            Code = code;
        }
    }
}
