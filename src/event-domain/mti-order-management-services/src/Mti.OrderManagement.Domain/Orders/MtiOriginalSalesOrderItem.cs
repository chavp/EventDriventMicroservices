using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.OrderManagement.Domain.Orders
{
    [Table("MtiOriginalSalesOrderItems")]
    [Index(nameof(OriginalId), IsUnique = true)]
    [Index(nameof(Status))]
    public sealed class MtiOriginalSalesOrderItem : SalesOrderItem
    {
        internal MtiOriginalSalesOrderItem(string status, 
            string? productName, string? policyType, 
            string? campaign, string? package, string? workshop)
        {
            Status = status;

            // products
            ProductName = productName;
            PolicyType = policyType;
            Campaign = campaign;
            Package = package;
            Workshop = workshop;
        }

        public uint? OriginalId { get; set; }

        [StringLength(100)]
        public string? TransID { get; set; }

        [Required, StringLength(5)]
        public string Status { get; set; }

        [Required, StringLength(200)]
        public string ProductName { get; set; }

        [Required, StringLength(200)]
        public string PolicyType { get; set; }

        [StringLength(100)]
        public string? PolicyNumber { get; set; }

        [StringLength(100)]
        public string? Campaign { get; set; }

        [StringLength(100)]
        public string? PolicyPreviousNumber { get; set; }
        public DateOnly? PolicyEffectiveDate { get; set; }
        public DateOnly? PolicyExpiryDate { get; set; }

        [StringLength(200)]
        public string? RefPolicyType { get; set; }

        [StringLength(1000)]
        public string? Remark { get; set; }

        [StringLength(100)]
        public string? RefNoticeNo { get; set; }

        [StringLength(100)]
        public string? RefDetailNo { get; set; }

        [StringLength(300)]
        public string? StatusMessage { get; set; }

        [StringLength(50)]
        public string? RefQuotation { get; set; }

        [StringLength(200)]
        public string? Source { get; set; }

        [StringLength(50)]
        public string? SystemId { get; set; }

        [StringLength(50)]
        public string? CustomerInfoNo { get; set; }

        [StringLength(50)]
        public string? Package { get; set; }

        [StringLength(10)]
        public string? Workshop { get; set; }

        [StringLength(50)]
        public string? PayPlan { get; set; }

        [StringLength(50)]
        public string? CollateralNo { get; set; }

        [StringLength(10)]
        public string? VehicleCode { get; set; }

        [StringLength(300)]
        public string? VehicleBrand { get; set; }
        [StringLength(600)]
        public string? VehicleModel { get; set; }

        public ushort? VehicleManufactoringYear { get; set; }

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


        public Guid? InsuredAssetId { get; set; }
        public InsuredAsset? InsuredAsset { get; set; }

        public decimal SumInsure { get; set; }
        public decimal Deduct { get; set; }
        public decimal DamageLifePerPerson { get; set; }
        public decimal DamageLifePerTime { get; set; }
        public decimal DamageInsurePerTime { get; set; }
        public decimal AccidentPerDriver { get; set; }
        public decimal MedicalInsure { get; set; }
        public decimal InsureDriver { get; set; }

        public string GetProductName()
        {
            return $"{PolicyType} {ProductName} {Campaign} {Package} {Workshop}";
        }
    }
}
