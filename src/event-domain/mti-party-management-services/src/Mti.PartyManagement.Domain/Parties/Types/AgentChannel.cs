using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties.Types
{
    [Table("AgentChannels")]
    [Index(nameof(Code), IsUnique = true)]
    public class AgentChannel : TypeModel
    {
        protected AgentChannel() { }
        public AgentChannel(string code)
        {
            Code = code;
        }
    }
}
