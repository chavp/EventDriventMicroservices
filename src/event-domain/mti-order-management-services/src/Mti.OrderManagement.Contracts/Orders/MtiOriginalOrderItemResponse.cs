using Mti.OrderManagement.Messaging;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record MtiOriginalOrderItemResponse
        : MtiOriginalOrderItemMessage
    {
        public MtiOriginalOrderItemResponse(Builder builder)
        {
            OrderItemTypeCode = builder.OrderItemTypeCode;
            Seq = builder.Seq;
            OrderItemId = builder.OrderItemId;
            NetPremium = builder.NetPremium;
            Quantity = builder.Quantity;

            Application = builder.Application;
            Product = builder.Product;
            Policy = builder.Policy;
        }

        public static Builder CreateBuilder(string applicationTypeCode) => new(applicationTypeCode);

        public new MtiOriginalApplicationValue? Application { get; }
        public new MtiOriginalProductValue? Product { get; }
        public new MtiOriginalCoverageValue? Coverage { get; }

        public new PolicyValue? Policy { get; set; }

        public new AssetValue? InsuredAsset { get; set; }
        public new IReadOnlyList<ExtractPartyValue> Parties { get; set; } = [];

        public sealed class Builder
        {
            internal string OrderItemTypeCode { get; private set; }
            internal uint Seq { get; private set; }
            internal Guid? OrderItemId { get; private set; }
            internal decimal NetPremium { get; private set; }
            internal uint Quantity { get; private set; } = 1;

            internal MtiOriginalApplicationValue? Application { get; private set; }
            internal MtiOriginalProductValue? Product { get; private set; }
            internal MtiOriginalCoverageValue? Coverage { get; private set; }

            internal PolicyValue? Policy { get; private set; }

            internal Builder(string orderItemTypeCode)
            {
                OrderItemTypeCode = orderItemTypeCode ?? throw new ArgumentNullException(nameof(OrderItemTypeCode));
            }

            public Builder WithSeq(uint seq)
            {
                Seq = seq;
                return this;
            }
            public Builder WithOrderItemId(Guid? orderItemId)
            {
                OrderItemId = orderItemId;
                return this;
            }
            public Builder WithNetPremium(decimal netPremium)
            {
                NetPremium = netPremium;
                return this;
            }
            public Builder WithQuantity(uint quantity)
            {
                Quantity = quantity;
                return this;
            }

            // application
            public Builder WithApplication(
                string applicationTypeCode,
                Func<MtiOriginalApplicationValue.Builder, MtiOriginalApplicationValue.Builder> builderFunc)
            {
                Application = builderFunc(MtiOriginalApplicationValue.CreateBuilder(applicationTypeCode)).Build();
                return this;
            }

            public Builder WithPolicy(
                Func<PolicyValue.Builder, PolicyValue.Builder> builderFunc)
            {
                Policy = builderFunc(PolicyValue.CreateBuilder())
                    .Build();
                return this;
            }

            public Builder WithProduct(
                 string productTypeCode,
                 string productName,
                 string policyType,
                Func<MtiOriginalProductValue.Builder, MtiOriginalProductValue.Builder> builderFunc)
            {
                Product = builderFunc(
                    MtiOriginalProductValue.CreateBuilder(
                        productTypeCode,
                        productName,
                        policyType))
                    .Build();
                return this;
            }

            public Builder WithCoverage(
                string coverageType,
                Func<MtiOriginalCoverageValue.Builder, MtiOriginalCoverageValue.Builder> builderFunc)
            {
                Coverage = builderFunc(MtiOriginalCoverageValue.CreateBuilder(coverageType)).Build();
                return this;
            }

            public MtiOriginalOrderItemResponse Build()
            {
                return new MtiOriginalOrderItemResponse(this);
            }
        }
    }
}
