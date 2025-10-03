using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("CoverageAmounts")]
    public class CoverageAmount : CoverageLevel
    {
        public CoverageAmount(Guid? coverageLevelTypeId, Guid? coverageLevelBasisId)
            : base(coverageLevelTypeId, coverageLevelBasisId)
        {
        }

        public decimal Amount { get; set; }
    }
}
