using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Guards;
using Mti.OrderManagement.Contracts.Extensions;
using Mti.OrderManagement.Messaging;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record MtiOriginalProductValue
        : MtiOriginalProductMessage
    {
        public MtiOriginalProductValue(Builder builder)
        {
            ProductTypeCode = builder.ProductTypeCode;
            ProductId = builder.ProductId;
            ProductName = builder.ProductName;
            PolicyType = builder.PolicyType;
            Campaign = builder.Campaign;
            Package = builder.Package;
            Workshop = builder.Workshop;
            RefPolicyType = builder.RefPolicyType;
            Name = builder.Name;
            Code = builder.Code;
        }

        public static Builder CreateBuilder(
                string productTypeCode,
                string productName,
                string policyType) => new(productTypeCode, productName, policyType);

        public sealed class Builder
        {
            internal Guid? ProductId { get; private set; }
            internal string ProductTypeCode { get; private set; }
            internal string ProductName { get; private set; }
            internal string PolicyType { get; private set; }
            internal string? Campaign { get; private set; }
            internal string? Package { get; private set; }
            internal string? Workshop { get; private set; }

            internal string? RefPolicyType { get; private set; }

            internal string? Name
            {
                get
                {
                    return $"{PolicyType} {ProductName} {Campaign} {Package} {Workshop}";
                }
            }

            internal string? Code
            {
                get
                {
                    return Name.GenCode();
                }
            }

            public Builder(
                string productTypeCode,
                string productName,
                string policyType)
            {
                ProductTypeCode = productTypeCode;
                Ensure.NotEmpty(productName, "ProductName is required.", nameof(ProductName));
                Ensure.NotEmpty(policyType, "PolicyType is required.", nameof(PolicyType));

                ProductName = productName;
                PolicyType = policyType;
            }

            public MtiOriginalProductValue Build() => new(this);

            public Builder WithProductId(Guid? productId)
            {
                ProductId = productId;
                return this;
            }
            public Builder WithCampaign(string? campaign)
            {
                Campaign = campaign;
                return this;
            }
            public Builder WithPackage(string? package)
            {
                Package = package;
                return this;
            }
            public Builder WithWorkshop(string? workshop)
            {
                Workshop = workshop;
                return this;
            }
            public Builder WithRefPolicyType(string? refPolicyType)
            {
                RefPolicyType = refPolicyType;
                return this;
            }
        }
    }
}
