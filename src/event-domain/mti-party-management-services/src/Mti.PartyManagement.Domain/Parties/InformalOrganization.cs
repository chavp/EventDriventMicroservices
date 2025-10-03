using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("InformalOrganizations")]
    [Index(nameof(Reference))]
    public class InformalOrganization : Organization
    {

    }
}
