using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("OptionalCoverages")]
    public class OptionalCoverage : CoverageAvailability
    {
        public OptionalCoverage(Guid? productId,
            Guid? coverageTypeId,
            Guid? coverageLevelId) : base(productId, coverageTypeId, coverageLevelId)
        {
        }
        // Additional properties or methods can be added here if needed
    }
}
