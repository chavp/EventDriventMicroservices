using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Guards;
using Mti.OrderManagement.Messaging;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record AssetValue
        : AssetMessage
    {
        public AssetValue(Builder builder)
        {
            AssetTypeCode = builder.AssetTypeCode;
            AssetId = builder.AssetId;
            InsuredAssetId = builder.InsuredAssetId;
            Description = builder.Description;
            Vehicle = builder.Vehicle;
        }

        public static Builder CreateBuilder(string assetTypeCode) => new(assetTypeCode);

        public sealed class Builder
        {
            public string? AssetTypeCode { get; private set; }
            public Guid? AssetId { get; private set; }
            public Guid? InsuredAssetId { get; private set; }
            public string? Description { get; private set; }
            public VehicleValue? Vehicle { get; private set; }

            public Builder WithAssetId(Guid? assetId)
            {
                AssetId = assetId;
                return this;
            }
            public Builder WithInsuredAssetId(Guid? insuredAssetId)
            {
                InsuredAssetId = insuredAssetId;
                return this;
            }
            public Builder WithDescription(string? description)
            {
                Description = description;
                return this;
            }

            public Builder WithVehicle(
                Func<VehicleValue.Builder, VehicleValue.Builder> builderFunc)
            {
                Vehicle = builderFunc(VehicleValue.CreateBuilder())
                    .Build();
                return this;
            }

            public Builder(string assetTypeCode)
            {
                AssetTypeCode = assetTypeCode;
            }

            public AssetValue? Build() => new AssetValue(this);
        }
    }
}
