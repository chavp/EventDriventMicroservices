using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("People")]
    [Index(nameof(FirstName), nameof(LastName))]
    [Index(nameof(CardId))]
    public class Person : Party
    {
        [StringLength(200)]
        public string? FirstName { get; set; }
        [StringLength(200)]
        public string? MiddleName { get; set; }

        [StringLength(300)] 
        public string? LastName { get; set; }

        [StringLength(50)]
        public string? CardId { get; set; }

        public DateOnly? BirthDate { get; set; }
        public ushort? Height { get; set; }
        public ushort? Weight { get; set; }
    }
}
