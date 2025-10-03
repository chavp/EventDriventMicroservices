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
    [Table("VehicleModels")]
    public sealed class VehicleModel : ProductFeature
    {
        private VehicleModel()
        {
        }
        public VehicleModel(Guid productFeatureTypeId,
            string code)
        {
            ProductFeatureTypeId = productFeatureTypeId;
            Code = code;
        }
    }
}
