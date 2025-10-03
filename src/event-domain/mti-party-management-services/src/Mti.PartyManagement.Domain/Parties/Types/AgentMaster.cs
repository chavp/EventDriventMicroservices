using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties.Types
{
    [Table("AgentMasters")]
    [Index(nameof(Code), IsUnique = true)]
    public class AgentMaster : TypeModel
    {
        protected AgentMaster() { }
        public AgentMaster(string code)
        {
            Code = code;
        }
    }
}
