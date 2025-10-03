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
    [Table("ProductFeatureAvailabilities")]
    [Index(nameof(ProductId), nameof(ProductFeatureId), IsUnique = true)]
    public abstract class ProductFeatureAvailability : EffectiveModel
    {
        public ProductFeatureAvailability(Guid? productId,
            Guid? productFeatureId)
        {
            ProductId = productId;
            ProductFeatureId = productFeatureId;
        }

        [Required]
        public Guid? ProductId { get; set; }
        public Product? Product { get; set; }

        [Required]
        public Guid? ProductFeatureId { get; set; }
        public ProductFeature? ProductFeature { get; set; }

    }
}
