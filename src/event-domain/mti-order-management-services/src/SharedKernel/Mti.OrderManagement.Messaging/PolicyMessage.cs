using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Messaging
{
    public record PolicyMessage
    {
        public Guid? AgreementId { get; set; }
        public string? Number { get; set; }
    }
}
