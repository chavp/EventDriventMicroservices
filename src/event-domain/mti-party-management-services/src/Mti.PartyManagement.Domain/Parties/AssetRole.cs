using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Mti.PartyManagement.Domain.Parties.Types;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("AssetRoles")]
    public class AssetRole : EffectiveModel
    {
        protected AssetRole() { }
        public AssetRole(Guid? partyId,
            Guid? assetRoleTypeId,
            Guid? assetId)
        {
            PartyId = partyId;
            AssetRoleTypeId = assetRoleTypeId;
            AssetId = assetId;
        }

        [Required]
        public Guid? PartyId { get; set; }
        public Party? Party { get; set; }

        [Required]
        public Guid? AssetRoleTypeId { get; set; }
        public AssetRoleType? AssetRoleType { get; set; }

        [Required]
        public Guid? AssetId { get; set; }
        public Asset? Asset { get; set; }
    }
}
