using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Contracts.Orders;
using Mti.OrderManagement.Contracts.Extensions;

namespace Mti.OrderManagement.Contracts.Builders
{
    public sealed class MtiOriginalApplicationValueBuilder
    {
        internal string ApplicationType { get; }
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
        internal string? PolicyPreviousNumber { get; private set; }
        internal DateOnly? PolicyEffectiveDate { get; private set; }
        internal DateOnly? PolicyExpiryDate { get; private set; }

        internal MtiOriginalApplicationValueBuilder(string applicationType)
        {
            ApplicationType = applicationType ?? throw new ArgumentNullException(nameof(applicationType));
        }

        public MtiOriginalApplicationValueBuilder WithOriginalId(uint? originalId)
        {
            OriginalId = originalId;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithTransID(string? transId)
        {
            TransID = transId;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithStatus(string? status)
        {
            Status = status;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithRemark(string? remark)
        {
            Remark = remark;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithSource(string? source)
        {
            Source = source;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithSystemId(string? systemId)
        {
            SystemId = systemId;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithRefNoticeNo(string? refNoticeNo)
        {
            RefNoticeNo = refNoticeNo;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithRefDetailNo(string? refDetailNo)
        {
            RefDetailNo = refDetailNo;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithStatusMessage(string? statusMessage)
        {
            StatusMessage = statusMessage;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithRefQuotation(string? refQuotation)
        {
            RefQuotation = refQuotation;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithPayPlan(string? payPlan)
        {
            PayPlan = payPlan;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithCollateralNo(string? collateralNo)
        {
            CollateralNo = collateralNo;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithCustomerInfoNo(string? customerInfoNo)
        {
            CustomerInfoNo = customerInfoNo;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithPolicyPreviousNumber(string? policyPreviousNumber)
        {
            PolicyPreviousNumber = policyPreviousNumber;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithPolicyEffectiveDate(DateOnly? effectiveDate)
        {
            PolicyEffectiveDate = effectiveDate;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithPolicyExpiryDate(DateOnly? expiryDate)
        {
            PolicyExpiryDate = expiryDate;
            return this;
        }

        // Convenience methods for date handling
        public MtiOriginalApplicationValueBuilder WithPolicyEffectiveDate(DateTime? effectiveDate)
        {
            PolicyEffectiveDate = effectiveDate?.ToDateOnly();
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithPolicyExpiryDate(DateTime? expiryDate)
        {
            PolicyExpiryDate = expiryDate?.ToDateOnly();
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithPolicyDates(DateOnly? effectiveDate, DateOnly? expiryDate)
        {
            PolicyEffectiveDate = effectiveDate;
            PolicyExpiryDate = expiryDate;
            return this;
        }

        public MtiOriginalApplicationValueBuilder WithPolicyDates(DateTime? effectiveDate, DateTime? expiryDate)
        {
            PolicyEffectiveDate = effectiveDate?.ToDateOnly();
            PolicyExpiryDate = expiryDate?.ToDateOnly();
            return this;
        }

        // Validation method (optional)
        public MtiOriginalApplicationValueBuilder ValidatePolicyDates()
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
