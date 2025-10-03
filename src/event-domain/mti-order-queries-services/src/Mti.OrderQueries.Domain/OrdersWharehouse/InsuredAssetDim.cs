using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.OrderQueries.Domain.OrdersWharehouse
{
    [Index(nameof(AssetTypeCode))]
    public class InsuredAssetDim : StarModel
    {
        [Required, StringLength(300)]
        public string? AssetTypeCode { get; set; }

        public Guid? AssetId{ get; set; }

        [StringLength(300)]
        public string? AssetName { get; set; }

        [StringLength(300)]
        public string? VehicleBrand { get; set; }

        [StringLength(600)]
        public string? VehicleModel { get; set; }

        [StringLength(50)]
        public string? VehicleColor { get; set; }

        [StringLength(100)]
        public string? VehicleRegisterNo { get; set; }

        [StringLength(100)]
        public string? VehicleRegisterProvince { get; set; }

        public ushort? VehicleRegisterYear { get; set; }

        [StringLength(50)]
        public string? VehicleChassis { get; set; }

        public float? VehicleCc { get; set; }
        public float? VehicleSeat { get; set; }
        public float? VehicleWeight { get; set; }
        public float? VehicleTonnage { get; set; }

        [StringLength(100)]
        public string? VehicleEngine { get; set; }
        public ushort? VehiclePassenger { get; set; }
    }
}
