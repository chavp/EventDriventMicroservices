using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Messaging
{
    public record MtiOriginalApplicationMessage
    {
        public string? ApplicationTypeCode { get; set; }
        public uint? OriginalId { get; set; }
        public string? TransID { get; set; }
        public string? Status { get; set; }
        public string? Remark { get; set;  }
        public string? Source { get; set; }
        public string? SystemId { get; set; }
        public string? RefNoticeNo { get; set; }
        public string? RefDetailNo { get; set; }
        public string? StatusMessage { get; set; }
        public string? RefQuotation { get; set; }
        public string? PayPlan { get; set; }
        public string? CollateralNo { get; set; }
        public string? CustomerInfoNo { get; set; }
        public string? PolicyType { get; set; }
        public string? PolicyNumber { get; set; }
        public string? PolicyPreviousNumber { get; set; }
        public DateOnly? PolicyEffectiveDate { get; set; }
        public DateOnly? PolicyExpiryDate { get; set; }
    }
}
