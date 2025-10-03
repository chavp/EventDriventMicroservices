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
    [Table("Organizations")]
    [Index(nameof(Name))]
    [Index(nameof(Reference))]
    public abstract class Organization : Party
    {
        [StringLength(400)]
        public string? Name { get; set; }

        [StringLength(500)]
        public string? Reference { get; set; }
    }
}
