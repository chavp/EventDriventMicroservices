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
    [Table("VehicleBrands")]
    public sealed class VehicleBrand : ProductFeature
    {
        private VehicleBrand()
        {
        }
        public VehicleBrand(Guid productFeatureTypeId,
            string code)
        {
            ProductFeatureTypeId = productFeatureTypeId;
            Code = code;
        }
    }
}
