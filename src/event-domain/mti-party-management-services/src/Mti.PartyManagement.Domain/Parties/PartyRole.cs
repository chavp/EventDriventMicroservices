using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("PartyRoles")]
    [Index(nameof(PartyId), nameof(PartyRoleTypeId), IsUnique = true)]
    public abstract class PartyRole : EffectiveModel
    {
        [Required]
        public Guid? PartyId { get; set; }
        public Party? Party { get; set; }

        [Required]
        public Guid? PartyRoleTypeId { get; set; }
        public PartyRoleType? PartyRoleType { get; set; }
    }
}
