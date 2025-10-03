using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.OrderQueries.Domain.OrdersWharehouse
{
    [Index(nameof(ProductCode), nameof(Products_TenantId), IsUnique = true)]
    [Index(nameof(ProductCategoryTypeCodes))]
    public class ProductDim : StarModel
    {
        [StringLength(200)]
        public string? Products_TenantId { get; set; }

        public Guid? ProductId { get; set; }

        [Required, StringLength(1000)]
        public string? ProductCode { get; set; }

        [StringLength(300)]
        public string? ProductName { get; set; }

        [StringLength(5000)]
        public string? ProductCategoryTypeCodes { get; set; }

        [StringLength(100)]
        public string? ProductCampaign { get; set; }

        [StringLength(50)]
        public string? ProductPackage { get; set; }

        [StringLength(10)]
        public string? ProductWorkshop { get; set; }
        [StringLength(200)]
        public string? ProductRefPolicyType { get; set; }
    }
}
