using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("Deductibilities")]
    public class Deductibility : CoverageLevel
    {
        public Deductibility(Guid? coverageLevelTypeId, Guid? coverageLevelBasisId)
            : base(coverageLevelTypeId, coverageLevelBasisId)
        {
        }

        public decimal Amount { get; set; }
    }
}
