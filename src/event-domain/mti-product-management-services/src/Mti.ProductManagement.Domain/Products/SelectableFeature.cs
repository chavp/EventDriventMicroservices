using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("SelectableFeatures")]
    public class SelectableFeature : ProductFeatureAvailability
    {
        public SelectableFeature(Guid? productId,
           Guid? productFeatureId) : base(productId, productFeatureId)
        {

        }
    }
}
