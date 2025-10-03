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
    [Table("VehicleYears")]
    public sealed class VehicleYear : ProductFeature
    {
        private VehicleYear()
        {
        }
        public VehicleYear(Guid productFeatureTypeId,
            string code)
        {
            ProductFeatureTypeId = productFeatureTypeId;
            Code = code;
        }
    }
}
