using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record TranformMtiOriginalItemRequest
    {
        public uint? ID { get; set; }
        public string? TransID { get; set; }
        public string? ProductName { get; set; }
        public string? PolicyType { get; set; }
        public string? Status { get; set; }

        // product
        public string? RefPolicyType { get; set; }
        public string? Campaign { get; set; }
        public string? Package { get; set; }
        public string? Workshop { get; set; }

        public decimal TotalPremium { get; set; }
        public decimal NetPremium { get; set; }

        public string? Remark { get; set; }
        public string? RefNoticeNo { get; set; }
        public string? RefDetailNo { get; set; }
        public string? RefQuotation { get; set; }
        public string? Source { get; set; }
        public string? SystemId { get; set; }
        public string? StatusMessage { get; set; }
        public string? CustomerInfoNo { get; set; }

        // OWNER
        public TranformMtiOriginalItemPartyValue? Owner { get; set; }

        // INV
        public TranformMtiOriginalItemPartyValue? Invoice { get; set; }

        // Coverage
        public decimal SumInsure { get; set; }
        public decimal Deduct { get; set; }

        public decimal DamageLifePerPerson { get; set; }
        public decimal DamageLifePerTime { get; set; }
        public decimal DamageInsurePerTime { get; set; }
        public decimal AccidentPerDriver { get; set; }
        public decimal MedicalInsure { get; set; }
        public decimal InsureDriver { get; set; }

        // Vehicles
        public string? VehCode { get; set; }
        public string? BrandName { get; set; }
        public string? ModelName { get; set; }
        public ushort? Yrmanuf { get; set; }
        public string? RegNo { get; set; }
        public string? Engine { get; set; }
        public string? Chassis { get; set; }
        public string? RegProvince { get; set; }
        public float? Cc { get; set; }
        public ushort? Seat { get; set; }
        public float? Weight { get; set; }
        public float? Toannage { get; set; }
        public ushort? Passenger { get; set; }
        public string? PayPlan { get; set; }
        public string? CollateralNo { get; set; }
        public string? CarColour { get; set; }

        // policy
        public string? PolicyNo { get; set; }
        public string? OldPolicy { get; set; }

        public DateOnly? EffectiveDate { get; set; }
        public DateOnly? ExpiryDate { get; set; }

        public List<string> Errors { get; set; } = [];
    }

}
