using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties.Types
{
    [Table("AssetRoleTypes")]
    [Index(nameof(Code), IsUnique = true)]
    public class AssetRoleType : TypeModel
    {
        public const string Owner = "OWNER";

        protected AssetRoleType() { }
        public AssetRoleType(string code)
        {
            Code = code;
        }
    }
}
