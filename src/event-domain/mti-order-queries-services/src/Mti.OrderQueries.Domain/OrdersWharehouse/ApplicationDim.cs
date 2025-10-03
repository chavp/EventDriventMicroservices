using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderQueries.Domain.OrdersWharehouse
{
    public class ApplicationDim : StarModel
    {
        public uint? ApplicationOriginalId { get; set; }

        [StringLength(100)]
        public string? ApplicationTransID { get; set; }

        [StringLength(5)]
        public string? ApplicationStatus { get; set; }

        [StringLength(200)]
        public string? ApplicationPolicyType { get; set; }

        [StringLength(100)]
        public string? ApplicationPolicyNumber { get; set; }

        [StringLength(100)]
        public string? ApplicationPolicyPreviousNumber { get; set; }
        public DateOnly? ApplicationPolicyEffectiveDate { get; set; }
        public DateOnly? ApplicationPolicyExpiryDate { get; set; }


        [StringLength(1000)]
        public string? ApplicationRemark { get; set; }

        [StringLength(100)]
        public string? ApplicationRefNoticeNo { get; set; }

        [StringLength(100)]
        public string? ApplicationRefDetailNo { get; set; }

        [StringLength(300)]
        public string? ApplicationStatusMessage { get; set; }

        [StringLength(50)]
        public string? ApplicationRefQuotation { get; set; }

        [StringLength(200)]
        public string? ApplicationSource { get; set; }

        [StringLength(50)]
        public string? ApplicationSystemId { get; set; }

        [StringLength(50)]
        public string? ApplicationCustomerInfoNo { get; set; }

        [StringLength(50)]
        public string? ApplicationPayPlan { get; set; }

        [StringLength(50)]
        public string? ApplicationCollateralNo { get; set; }
    }
}
