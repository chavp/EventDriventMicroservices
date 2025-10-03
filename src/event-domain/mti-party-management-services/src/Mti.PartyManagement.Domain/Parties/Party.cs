using System.ComponentModel.DataAnnotations.Schema;
using Mti.PartyManagement.Domain.Parties.Types;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("Parties")]
    public abstract class Party : Entity
    {
        public Guid? PartyTitleId { get; set; }
        public PartyTitle? PartyTitle { get; set; }

        public Guid? NationalityId { get; set; }
        public Nationality? Nationality { get; set; }

        public List<AssetRole> AssetRoles { get; set; } = [];

        public List<ContactMechanism> ContactMechanisms { get; set; } = [];
        public List<PartyContactMechanism> PartyContactMechanisms { get; set; } = [];
    }
}

