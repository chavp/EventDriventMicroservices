using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Mti.PartyManagement.Domain.Parties.Types;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("ContactMechanisms")]
    public abstract class ContactMechanism : Entity
    {
        protected ContactMechanism() { }
        public ContactMechanism(Guid? contactMechanismTypeId)
        {
            ContactMechanismTypeId = contactMechanismTypeId;
        }

        public ushort Seq { get; set; }

        [Required]
        public Guid? ContactMechanismTypeId { get; set; }
        public ContactMechanismType? ContactMechanismType { get; set; }

        public List<Party> Parties { get; set; } = [];
        public List<PartyContactMechanism> PartyContactMechanisms { get; set; } = [];
    }
}
