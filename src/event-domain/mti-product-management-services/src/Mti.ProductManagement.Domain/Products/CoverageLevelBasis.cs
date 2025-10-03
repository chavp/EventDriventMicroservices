using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.ProductManagement.Domain.Products
{
    [Table("CoverageLevelBasises")]
    [Index(nameof(Code), IsUnique = true)]
    public class CoverageLevelBasis : TypeModel
    {
        public const string PerIncident = "PER_INCIDENT";
        public const string PerPerson = "PER_PERSON";
        public const string PerDisablitity = "PER_DISABLITITY";
        public const string PerDriver = "PER_DRIVER";
        public const string PerTime = "PER_TIME";
    }
}
