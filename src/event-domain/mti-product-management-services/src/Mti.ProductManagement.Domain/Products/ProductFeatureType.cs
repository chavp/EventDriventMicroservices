using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("ProductFeatureTypes")]
    [Index(nameof(Code), IsUnique = true)]
    public sealed class ProductFeatureType : TypeModel
    {
        public const string VehicleCode = "VEHICLE_CODE";
        public const string VehicleBrand = "VEHICLE_BRAND";
        public const string VehicleModel = "VEHICLE_MODEL";
        public const string VehicleYear = "VEHICLE_YEAR";
    }
}
