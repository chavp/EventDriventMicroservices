using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("CoverageLevelTypes")]
    [Index(nameof(Code), IsUnique = true)]
    public class CoverageLevelType : TypeModel
    {
        public const string CoverageAmount = "COVERAGE_AMOUNT";
        public const string Deductibility = "DEDUCTIBILITY";
    }
}
