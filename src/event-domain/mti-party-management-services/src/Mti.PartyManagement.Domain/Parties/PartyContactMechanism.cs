using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("PartyContactMechanisms")]
    public class PartyContactMechanism : EffectiveModel
    {
        protected PartyContactMechanism() { }
        public PartyContactMechanism(Guid? partyId, Guid? contactMechanismId)
        {
            PartyId = partyId;
            ContactMechanismId = contactMechanismId;
        }

        [Required]
        public Guid? PartyId { get; set; }
        public Party? Party { get; set; }

        [Required]
        public Guid? ContactMechanismId { get; set; }
        public ContactMechanism? ContactMechanism { get; set; }

    }
}
