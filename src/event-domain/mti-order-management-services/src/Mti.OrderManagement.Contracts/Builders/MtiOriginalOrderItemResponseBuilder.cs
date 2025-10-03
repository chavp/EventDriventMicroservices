using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Contracts.Orders;

namespace Mti.OrderManagement.Contracts.Builders
{
    public sealed class MtiOriginalOrderItemResponseBuilder
    {
        internal string OrderItemType { get; private set; }
        internal uint Seq { get; private set; }
        internal Guid? OrderItemId { get; private set; }
        internal decimal NetPremium { get; private set; }
        internal uint Quantity { get; private set; } = 1;

        internal MtiOriginalOrderItemResponseBuilder(string orderItemType)
        {
            OrderItemType = orderItemType ?? throw new ArgumentNullException(nameof(orderItemType));
        }

        public MtiOriginalOrderItemResponseBuilder WithSeq(uint seq)
        {
            Seq = seq;
            return this;
        }
        public MtiOriginalOrderItemResponseBuilder WithOrderItemId(Guid? orderItemId)
        {
            OrderItemId = orderItemId;
            return this;
        }
        public MtiOriginalOrderItemResponseBuilder WithNetPremium(decimal netPremium)
        {
            NetPremium = netPremium;
            return this;
        }
        public MtiOriginalOrderItemResponseBuilder WithQuantity(uint quantity)
        {
            Quantity = quantity;
            return this;
        }

        public MtiOriginalOrderItemResponse Build()
        {
            return new MtiOriginalOrderItemResponse(this);
        }
    }
}
