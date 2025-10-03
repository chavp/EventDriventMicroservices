using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("CoverageAvailabilities")]
    [Index(nameof(ProductId), nameof(CoverageTypeId), nameof(CoverageLevelId), IsUnique = true)]
    public abstract class CoverageAvailability : EffectiveModel
    {
        public CoverageAvailability(Guid? productId, 
            Guid? coverageTypeId, 
            Guid? coverageLevelId)
        {
            ProductId = productId;
            CoverageTypeId = coverageTypeId;
            CoverageLevelId = coverageLevelId;
        }

        [Required]
        public Guid? ProductId { get; set; }
        public Product? Product { get; set; }

        [Required]
        public Guid? CoverageTypeId { get; set; }
        public CoverageType? CoverageType { get; set; }

        [Required]
        public Guid? CoverageLevelId { get; set; }
        public CoverageLevel? CoverageLevel { get; set; }
    }
}
