
using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;

namespace Mti.ProductManagement.Domain.Products
{
    [Index(nameof(Code), IsUnique = true)]
    [Index(nameof(Name))]
    public class Product : Entity
    {
        protected Product() { }
        public Product(string code) 
        {
            Code = code;
        }
        
        [Required, StringLength(1000)]
        public string? Code { get; set; }

        [StringLength(300)]
        public string? Name { get; set; }

        public List<ProductCategory> ProductCatogories { get; set; } = [];
        public List<ProductCategoryClassification> ProductCatogoryClassifications { get; set; } = [];

        public List<CoverageAvailability> CoverageAvailabilities { get; set; } = [];
        public List<ProductFeatureAvailability> ProductFeatureAvailabilities { get; set; } = [];
    }
}
