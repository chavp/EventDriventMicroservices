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
    [Table("ProductFeatures")]
    [Index(nameof(Code), nameof(ProductFeatureTypeId), IsUnique = true)]
    public abstract class ProductFeature : Entity
    {
        [Required, StringLength(256)]
        public string? Code { get; set; }

        [StringLength(300)]
        public string? Name { get; set; }

        [Required]
        public Guid? ProductFeatureTypeId { get; set; }
        public ProductFeatureType? ProductFeatureType { get; set; }
    }
}
