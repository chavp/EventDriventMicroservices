using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.PartyManagement.Messaging.Parties.GetPartiesByIds
{
    public record GetPartiesByIdsQuery
    {
        public IReadOnlyList<Guid> PartyIds { get; set; } = [];
    }
}
