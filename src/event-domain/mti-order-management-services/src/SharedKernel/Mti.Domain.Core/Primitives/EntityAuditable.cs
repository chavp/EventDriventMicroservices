using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Abstractions;

namespace Mti.Domain.Core.Primitives
{
    public abstract class EntityAuditable : Entity, IAuditableEntity
    {
        [StringLength(300)]
        public string CreatedBy { get; set; } = Environment.MachineName;

        [StringLength(300)]
        public string? ModifiedBy { get; set; }

        public DateTime CreatedOnUtc { get; set; }

        public DateTime? ModifiedOnUtc { get; set; }

        public uint Revision { get; set; }
    }
}
