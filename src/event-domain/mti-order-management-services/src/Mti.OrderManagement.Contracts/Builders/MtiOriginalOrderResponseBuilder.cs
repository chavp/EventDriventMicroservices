using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Contracts.Orders;

namespace Mti.OrderManagement.Contracts.Builders
{
    public sealed class MtiOriginalOrderResponseBuilder
    {
        internal string OrderType { get; }
        internal Guid? OrderId { get; private set; }
        internal string? Number { get; private set; }
        internal DateOnly? SaleDate { get; private set; }
        internal string? LoanNumber { get; private set; }
        internal long TotalQuantity { get; private set; }

        internal List<MtiOriginalOrderItemResponse> OrderItems { get; private set; } = [];

        public MtiOriginalOrderResponseBuilder(string orderType)
        {
            OrderType = orderType;
        }

        public MtiOriginalOrderResponseBuilder WithOrderId(Guid? orderId)
        {
            OrderId = orderId;
            return this;
        }
        public MtiOriginalOrderResponseBuilder WithNumber(string? number)
        {
            Number = number;
            return this;
        }
        public MtiOriginalOrderResponseBuilder WithSaleDate(DateOnly? saleDate)
        {
            SaleDate = saleDate;
            return this;
        }
        public MtiOriginalOrderResponseBuilder WithLoanNumber(string? loanNumber)
        {
            LoanNumber = loanNumber;
            return this;
        }
        public MtiOriginalOrderResponseBuilder WithTotalQuantity(long totalQuantity)
        {
            TotalQuantity = totalQuantity;
            return this;
        }

        public MtiOriginalOrderResponseBuilder AddOrderItem(MtiOriginalOrderItemResponse orderItem)
        {
            OrderItems.Add(orderItem);
            return this;
        }

        public MtiOriginalOrderResponse Build()
        {
            return new MtiOriginalOrderResponse(this);
        }
    }
}
