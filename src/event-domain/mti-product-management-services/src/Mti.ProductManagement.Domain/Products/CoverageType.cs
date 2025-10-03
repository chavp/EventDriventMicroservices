using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("CoverageTypes")]
    [Index(nameof(Code), IsUnique = true)]
    public sealed class CoverageType : TypeModel
    {
        public const string SumInsured = "SUM_INSURED";
        public const string Deductible = "DEDUCTIBLE";
        public const string DamageLife = "DAMAGE_LIFE";
        public const string DamageInsure = "DAMAGE_INSURE";
        public const string Accident = "ACCIDENT";
        public const string MedicalInsure = "MEDICAL_INSURE";
        public const string InsureDriver = "INSURE_DRIVER";
    }
}
