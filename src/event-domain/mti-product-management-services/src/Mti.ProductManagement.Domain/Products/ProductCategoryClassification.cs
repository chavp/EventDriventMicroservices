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
    [Table("ProductCatogoryClassifications")]
    [Index(nameof(ProductCatogoryId), nameof(ProductId), IsUnique = true)]
    public class ProductCategoryClassification : EffectiveModel
    {
        protected ProductCategoryClassification() { }
        public ProductCategoryClassification(Guid productCatogoryId, Guid productId)
        {
            ProductCatogoryId = productCatogoryId;
            ProductId = productId;
        }

        [Required]
        public Guid? ProductCatogoryId { get; set; }
        public ProductCategory? ProductCatogory { get; set; }

        [Required]
        public Guid? ProductId { get; set; }
        public Product? Product { get; set; }

    }
}
