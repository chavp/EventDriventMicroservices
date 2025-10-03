using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.ProductManagement.Domain.Products
{
    [Index(nameof(ParentProductCategoryId), nameof(Code), IsUnique = true)]
    public class ProductCategory : TypeModel
    {
        public const string Motor = "MOTOR";
        public const string MtiOriginal = "MTI_ORIGINAL";

        //public ProductCatogory() { }
        public Guid? ParentProductCategoryId { get; set; }
        public ProductCategory? ParentProductCategory { get; set; }

        public List<ProductCategory> Subcategories { get; set; } = [];

        public List<Product> Products { get; set; } = [];
        public List<ProductCategoryClassification> ProductCatogoryClassifications { get; set; } = [];
    }
}
