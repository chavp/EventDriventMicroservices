using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("InsuredParties")]
    public abstract class InsuredParty : PartyRole
    {
    }
}
