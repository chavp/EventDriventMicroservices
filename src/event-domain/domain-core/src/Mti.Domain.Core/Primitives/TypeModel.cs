using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Primitives;

namespace Mti.Domain.Core.Primitives
{
    public abstract class TypeModel : EntityAuditable
    {
        [StringLength(256)]
        public string? Code { get; set; }

        [StringLength(300)]
        public string? Name { get; set; }
    }
}
