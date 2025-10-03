using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Primitives;

namespace Mti.OrderManagement.Domain.Orders
{
    [Table("InsuredAssets")]
    public class InsuredAsset : EntityAuditable
    {
        protected InsuredAsset() { }
        public InsuredAsset(string description)
        {
            Description = description;
        }

        [Required, StringLength(3000)]
        public string? Description { get; set; }

        public decimal BookValue { get; set; }

        public Guid? Parties_AssetId { get; set; }
    }
}
