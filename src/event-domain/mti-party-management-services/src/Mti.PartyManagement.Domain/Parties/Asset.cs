using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("Assets")]
    [Index(nameof(Name))]
    public abstract class Asset : Entity
    {
        [Required, StringLength(300)]
        public string? Name { get; set; }
    }
}
