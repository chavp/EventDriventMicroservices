using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Messaging
{
    public record MtiOriginalProductMessage
    {
        public string? ProductTypeCode { get; set; }
        public Guid? ProductId { get; set; }
        public string? ProductName { get; set; }
        public string? PolicyType { get; set; }
        public string? Campaign { get; set; }
        public string? Package { get; set; }
        public string? Workshop { get; set; }
        public string? RefPolicyType { get; set; }
        public string? Name { get; set; }
        public string? Code { get; set; }
    }
}
