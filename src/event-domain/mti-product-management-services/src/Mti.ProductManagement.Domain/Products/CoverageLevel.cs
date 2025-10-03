using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("CoverageLevels")]
    public abstract class CoverageLevel : Entity
    {
        protected CoverageLevel() { }
        public CoverageLevel(Guid? coverageLevelTypeId, Guid? coverageLevelBasisId)
        {
            CoverageLevelTypeId = coverageLevelTypeId;
            CoverageLevelBasisId = coverageLevelBasisId;
        }

        [Required]
        public Guid? CoverageLevelTypeId { get; set; }
        public CoverageLevelType? CoverageLevelType { get; set; }

        [Required]
        public Guid? CoverageLevelBasisId { get; set; }
        public CoverageLevelBasis? CoverageLevelBasis { get; set; }

    }
}
