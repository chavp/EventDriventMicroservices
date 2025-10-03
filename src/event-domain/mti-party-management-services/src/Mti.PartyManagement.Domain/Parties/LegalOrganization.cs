using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("LegalOrganizations")]
    [Index(nameof(FederalTaxIdNumber))]
    public class LegalOrganization : Organization
    {
        protected LegalOrganization() { }
        public LegalOrganization(string federalTaxIdNumber)
        {
            FederalTaxIdNumber = federalTaxIdNumber;
        }

        [Required, StringLength(50)]
        public string? FederalTaxIdNumber { get; set; }
    }
}
