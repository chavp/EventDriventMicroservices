using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderQueries.Domain
{
    public abstract class StarModel
    {
        [Key]
        public Guid Key { get; set; }

        [StringLength(300)]
        public string CreatedBy { get; set; } = Environment.MachineName;

        [StringLength(300)]
        public string? ModifiedBy { get; set; }

        public DateTime CreatedOnUtc { get; set; }

        public DateTime? ModifiedOnUtc { get; set; }

        public uint Revision { get; set; }
    }
}
