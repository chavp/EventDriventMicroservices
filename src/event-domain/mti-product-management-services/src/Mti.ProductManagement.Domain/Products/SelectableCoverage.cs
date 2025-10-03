using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("SelectableCoverages")]
    public class SelectableCoverage : CoverageAvailability
    {
        public SelectableCoverage(Guid? productId,
            Guid? coverageTypeId,
            Guid? coverageLevelId) : base(productId, coverageTypeId, coverageLevelId)
        {

        }
    }
}
