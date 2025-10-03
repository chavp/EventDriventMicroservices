using System.Collections.ObjectModel;
using System.Globalization;
using Mti.OrderManagement.Messaging;
using static System.Net.Mime.MediaTypeNames;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record MtiOriginalOrderResponse
        : MtiOriginalOrderMessage
    {
        public new ReadOnlyCollection<MtiOriginalOrderItemResponse> OrderItems { get; }

        public MtiOriginalOrderResponse(Builder builder)
        {
            OrderTypeCode = builder.OrderType;
            OrderId = builder.OrderId;
            Number = builder.Number;
            SaleDate = builder.SaleDate;
            LoanNumber = builder.LoanNumber;
            TotalQuantity = builder.TotalQuantity;
            OrderItems = builder.OrderItems.AsReadOnly();
        }

        public static Builder CreateBuilder(
            string applicationTypeCode,
            DateOnly saleDate,
            string loanNumber) => new(applicationTypeCode, saleDate, loanNumber);

        public sealed class Builder
        {
            internal string Orders_TenantId { get; set; }
            internal string? Parties_TenantId { get; set; }
            internal string? Products_TenantId { get; set; }

            internal string OrderType { get; }
            internal Guid? OrderId { get; private set; }
            internal string? Number { get; private set; }
            internal DateOnly SaleDate { get; private set; }
            internal string LoanNumber { get; private set; }
            internal long TotalQuantity { get; private set; }

            internal List<MtiOriginalOrderItemResponse> OrderItems { get; private set; } = [];

            public Builder(string orderType, DateOnly saleDate, string loanNumber)
            {
                if(string.IsNullOrEmpty(orderType)) throw new ArgumentNullException(nameof(OrderType));
                if(string.IsNullOrEmpty(loanNumber)) throw new ArgumentNullException(nameof(LoanNumber));

                OrderType = orderType;
                SaleDate = saleDate;
                LoanNumber = loanNumber;
            }

            public Builder WithOrderId(Guid? orderId)
            {
                OrderId = orderId;
                return this;
            }

            public Builder WithOrders_TenantId(string? orders_TenantId)
            {
                Orders_TenantId = orders_TenantId;
                return this;
            }
            public Builder WithParties_TenantId(string? parties_TenantId)
            {
                Parties_TenantId = parties_TenantId;
                return this;
            }
            public Builder WithProducts_TenantId(string? products_TenantId)
            {
                Products_TenantId = products_TenantId;
                return this;
            }
            //public Builder WithSaleDate(DateOnly? saleDate)
            //{
            //    if (!SaleDate.HasValue) throw new ArgumentNullException("Required SaleDate", nameof(SaleDate));

            //    SaleDate = saleDate;
            //    return this;
            //}
            //public Builder WithLoanNumber(string? loanNumber)
            //{
            //    if (string.IsNullOrEmpty(LoanNumber)) throw new ArgumentNullException("Required LoanNumber", nameof(loanNumber));

            //    LoanNumber = loanNumber;
            //    return this;
            //}
            public Builder WithTotalQuantity(long totalQuantity)
            {
                TotalQuantity = totalQuantity;
                return this;
            }

            public Builder AddOrderItem(MtiOriginalOrderItemResponse orderItem)
            {
                OrderItems.Add(orderItem);
                return this;
            }

            public MtiOriginalOrderResponse Build()
            {
                Number = $"{SaleDate.ToString("ddMMyyyy", CultureInfo.InvariantCulture)}-{LoanNumber}";
                TotalQuantity = OrderItems.Sum(x => x.Quantity);
                return new MtiOriginalOrderResponse(this);
            }
        }
    }
}
