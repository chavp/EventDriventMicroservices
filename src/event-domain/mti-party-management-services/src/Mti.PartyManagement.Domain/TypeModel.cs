using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.PartyManagement.Domain
{
    public abstract class TypeModel : Entity
    {
        [StringLength(500)]
        public string? Code { get; set; }

        [StringLength(1000)]
        public string? Name { get; set; }
    }
}
