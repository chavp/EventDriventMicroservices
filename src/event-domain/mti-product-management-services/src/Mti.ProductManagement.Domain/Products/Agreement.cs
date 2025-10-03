using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("Agreements")]
    public class Agreement : EffectiveModel
    {
        protected Agreement() { }

        [Required]
        public Guid? ProductId { get; set; }

    }
}
