using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Mti.PartyManagement.Domain.Parties.Types;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("Agents")]
    [Index(nameof(Name))]
    [Index(nameof(Number), nameof(ClientNumber), IsUnique = true)]
    public class Agent : PartyRole
    {
        [Required, StringLength(200)]
        public string? Name { get; set; }

        [Required, StringLength(20)]
        public string? Number { get; set; }

        [StringLength(20)]
        public string? License { get; set; }

        [StringLength(20)]
        public string? StaffCode { get; set; }

        [StringLength(20)]
        public string? ClientNumber { get; set; }

        public Guid? AgentChannelId { get; set; }
        public AgentChannel? AgentChannel { get; set; }

        public Guid? AgentMasterId { get; set; }
        public AgentMaster? AgentMaster { get; set; }

        [StringLength(100)]
        public string? ConfigCode { get; set; }

        [StringLength(200)]
        public string? Description { get; set; }
    }
}
