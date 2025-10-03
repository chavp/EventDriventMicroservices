using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Contracts.Orders
{
    using Mti.OrderManagement.Contracts.Extensions;
    using Mti.OrderManagement.Messaging;

    public sealed record MtiOriginalApplicationValue
        : MtiOriginalApplicationMessage
    {
        public MtiOriginalApplicationValue(Builder builder)
        {
            ApplicationTypeCode = builder.ApplicationTypeCode;
            OriginalId = builder.OriginalId;
            TransID = builder.TransID;
            Status = builder.Status;
            Remark = builder.Remark;
            Source = builder.Source;
            SystemId = builder.SystemId;
            RefNoticeNo = builder.RefNoticeNo;
            RefDetailNo = builder.RefDetailNo;
            StatusMessage = builder.StatusMessage;
            RefQuotation = builder.RefQuotation;
            PayPlan = builder.PayPlan;
            CollateralNo = builder.CollateralNo;
            CustomerInfoNo = builder.CustomerInfoNo;
            PolicyType = builder.PolicyType;
            PolicyNumber = builder.PolicyNumber;
            PolicyPreviousNumber = builder.PolicyPreviousNumber;
            PolicyEffectiveDate = builder.PolicyEffectiveDate;
            PolicyExpiryDate = builder.PolicyExpiryDate;
        }

        public static Builder CreateBuilder(string applicationTypeCode) => new(applicationTypeCode);

        // builder 
        public sealed class Builder
        {
            internal string ApplicationTypeCode { get; }
            internal uint? OriginalId { get; private set; }
            internal string? TransID { get; private set; }
            internal string? Status { get; private set; }
            internal string? Remark { get; private set; }
            internal string? Source { get; private set; }
            internal string? SystemId { get; private set; }
            internal string? RefNoticeNo { get; private set; }
            internal string? RefDetailNo { get; private set; }
            internal string? StatusMessage { get; private set; }
            internal string? RefQuotation { get; private set; }
            internal string? PayPlan { get; private set; }
            internal string? CollateralNo { get; private set; }
            internal string? CustomerInfoNo { get; private set; }
            internal string? PolicyType { get; private set; }
            internal string? PolicyNumber { get; private set; }
            internal string? PolicyPreviousNumber { get; private set; }
            internal DateOnly? PolicyEffectiveDate { get; private set; }
            internal DateOnly? PolicyExpiryDate { get; private set; }

            internal Builder(string applicationTypeCode)
            {
                ApplicationTypeCode = applicationTypeCode ?? throw new ArgumentNullException(nameof(applicationTypeCode));
            }

            public Builder WithOriginalId(uint? originalId)
            {
                OriginalId = originalId;
                return this;
            }

            public Builder WithTransID(string? transId)
            {
                TransID = transId;
                return this;
            }

            public Builder WithStatus(string? status)
            {
                Status = status;
                return this;
            }

            public Builder WithRemark(string? remark)
            {
                Remark = remark;
                return this;
            }

            public Builder WithSource(string? source)
            {
                Source = source;
                return this;
            }

            public Builder WithSystemId(string? systemId)
            {
                SystemId = systemId;
                return this;
            }

            public Builder WithRefNoticeNo(string? refNoticeNo)
            {
                RefNoticeNo = refNoticeNo;
                return this;
            }

            public Builder WithRefDetailNo(string? refDetailNo)
            {
                RefDetailNo = refDetailNo;
                return this;
            }

            public Builder WithStatusMessage(string? statusMessage)
            {
                StatusMessage = statusMessage;
                return this;
            }

            public Builder WithRefQuotation(string? refQuotation)
            {
                RefQuotation = refQuotation;
                return this;
            }

            public Builder WithPayPlan(string? payPlan)
            {
                PayPlan = payPlan;
                return this;
            }

            public Builder WithCollateralNo(string? collateralNo)
            {
                CollateralNo = collateralNo;
                return this;
            }

            public Builder WithCustomerInfoNo(string? customerInfoNo)
            {
                CustomerInfoNo = customerInfoNo;
                return this;
            }

            public Builder WithPolicyType(string? policyType)
            {
                PolicyType = policyType;
                return this;
            }

            public Builder WithPolicyNumber(string? policyNumber)
            {
                PolicyNumber = policyNumber;
                return this;
            }

            public Builder WithPolicyPreviousNumber(string? policyPreviousNumber)
            {
                PolicyPreviousNumber = policyPreviousNumber;
                return this;
            }

            public Builder WithPolicyEffectiveDate(DateOnly? effectiveDate)
            {
                PolicyEffectiveDate = effectiveDate;
                return this;
            }

            public Builder WithPolicyExpiryDate(DateOnly? expiryDate)
            {
                PolicyExpiryDate = expiryDate;
                return this;
            }

            // Convenience methods for date handling
            public Builder WithPolicyEffectiveDate(DateTime? effectiveDate)
            {
                PolicyEffectiveDate = effectiveDate?.ToDateOnly();
                return this;
            }

            public Builder WithPolicyExpiryDate(DateTime? expiryDate)
            {
                PolicyExpiryDate = expiryDate?.ToDateOnly();
                return this;
            }

            public Builder WithPolicyDates(DateOnly? effectiveDate, DateOnly? expiryDate)
            {
                PolicyEffectiveDate = effectiveDate;
                PolicyExpiryDate = expiryDate;
                return this;
            }

            public Builder WithPolicyDates(DateTime? effectiveDate, DateTime? expiryDate)
            {
                PolicyEffectiveDate = effectiveDate?.ToDateOnly();
                PolicyExpiryDate = expiryDate?.ToDateOnly();
                return this;
            }

            // Validation method (optional)
            public Builder ValidatePolicyDates()
            {
                if (PolicyEffectiveDate.HasValue && PolicyExpiryDate.HasValue &&
                    PolicyEffectiveDate > PolicyExpiryDate)
                {
                    throw new InvalidOperationException("Policy effective date cannot be after expiry date");
                }
                return this;
            }

            //public Builder WithTranformMtiOriginalItemRequest(TranformMtiOriginalItemRequest request)
            //{
            //    return this
            //        .Wi;
            //}

            public MtiOriginalApplicationValue Build()
            {
                return new MtiOriginalApplicationValue(this);
            }

            // Implicit conversion for convenience
            //public static implicit operator MtiOriginalApplicationValue(MtiOriginalApplicationValueBuilder builder)
            //{
            //    return builder.Build();
            //}
        }
    }
}
