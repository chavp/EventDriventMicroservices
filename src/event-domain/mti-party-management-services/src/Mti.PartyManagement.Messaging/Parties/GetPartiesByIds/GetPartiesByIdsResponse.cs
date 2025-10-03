using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.PartyManagement.Messaging.Parties;

namespace Mti.PartyManagement.Messaging.Parties.GetPartiesByIds
{
    public record GetPartiesByIdsResponse
    {
        public string? Parties_TenantId { get; init; }

        public IReadOnlyList<PartyMessage> Data { get; init; } = [];
    }

}
