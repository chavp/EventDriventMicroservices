using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.PartyManagement.Messaging.Parties.GetAssetsByIds
{
    public record GetAssetsByIdsResponse
    {
        public string? Parties_TenantId { get; set; }

        public IReadOnlyList<AssetMessage> Data { get; set; } = [];
    }
}
