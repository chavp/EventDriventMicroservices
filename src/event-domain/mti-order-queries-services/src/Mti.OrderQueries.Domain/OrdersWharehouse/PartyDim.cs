using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.OrderQueries.Domain.OrdersWharehouse
{
    [Index(nameof(PartyTypeCode))]
    public class PartyDim : StarModel
    {
        [StringLength(200)]
        public string? Parties_TenantId { get; set; }

        [Required, StringLength(300)]
        public string? PartyTypeCode { get; set; }

        public Guid? PartyId { get; set; }

        [StringLength(400)]
        public string? OrganizationName { get; set; }

        [StringLength(500)]
        public string? OrganizationReference { get; set; }

        // Legal Organization
        [StringLength(50)]
        public string? LegalOrganizationFederalTaxIdNumber { get; set; }

        [StringLength(1000)]
        public string? PartyTitleName { get; set; }

        // Person
        [StringLength(200)]
        public string? PersonFirstName { get; set; }
        [StringLength(200)]
        public string? PersonMiddleName { get; set; }

        [StringLength(300)]
        public string? PersonLastName { get; set; }

        [StringLength(50)]
        public string? PersonCardId { get; set; }

        public DateOnly? PersonBirthDate { get; set; }
        public ushort? PersonHeight { get; set; }
        public ushort? PersonWeight { get; set; }

        public List<ContactMechanismDim> ContactMechanisms { get; set; } = [];
    }
}
