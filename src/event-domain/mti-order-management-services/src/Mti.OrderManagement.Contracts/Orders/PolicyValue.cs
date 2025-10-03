using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Messaging;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record PolicyValue
        : PolicyMessage
    {

        public PolicyValue(Builder builder) 
        { 
            AgreementId = builder.AgreementId;
            Number = builder.Number;
        }

        public static Builder CreateBuilder() => new();

        public sealed class Builder
        {
            public Guid? AgreementId { get; private set; }
            public string? Number { get; private set; }

            public Builder WithAgreementId(Guid agreementId)
            {
                AgreementId = agreementId;
                return this;
            }
            public Builder WithNumber(string? number)
            {
                Number = number;
                return this;
            }

            public PolicyValue? Build() 
            {
                if (string.IsNullOrEmpty(Number))
                {
                    return null;
                }

                return new PolicyValue(this);
            }
        }
    }
}
