using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.PartyManagement.Messaging.Parties.GetAssetsByIds
{
    public record GetAssetsByIdsQuery
    {
        public IReadOnlyList<Guid> AssetIds { get; set; } = [];
    }
}
