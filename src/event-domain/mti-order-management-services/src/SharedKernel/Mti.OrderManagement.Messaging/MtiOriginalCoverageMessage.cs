using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Messaging
{
    public record MtiOriginalCoverageMessage
    {
        public string? CoverageTypeCode { get; set; }
        public decimal SumInsure { get; set; }
        public decimal Deduct { get; set; }
        public decimal DamageLifePerPerson { get; set; }
        public decimal DamageLifePerTime { get; set; }
        public decimal DamageInsurePerTime { get; set; }
        public decimal AccidentPerDriver { get; set; }
        public decimal MedicalInsure { get; set; }
        public decimal InsureDriver { get; set; }
    }
}
