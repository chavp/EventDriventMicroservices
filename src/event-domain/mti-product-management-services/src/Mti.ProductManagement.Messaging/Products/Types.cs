using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.ProductManagement.Messaging.Products
{
    public static class Types
    {
        public static class CoverageTypes
        {
            public const string SumInsured = "SUM_INSURED";
            public const string Deductible = "DEDUCTIBLE";
            public const string DamageLife = "DAMAGE_LIFE";
            public const string DamageInsure = "DAMAGE_INSURE";
            public const string Accident = "ACCIDENT";
            public const string MedicalInsure = "MEDICAL_INSURE";
            public const string InsureDriver = "INSURE_DRIVER";
        }

        public static class CoverageLevelBasises
        {
            public const string PerIncident = "PER_INCIDENT";
            public const string PerPerson = "PER_PERSON";
            public const string PerDisablitity = "PER_DISABLITITY";
            public const string PerDriver = "PER_DRIVER";
            public const string PerTime = "PER_TIME";
        }

        public static class CoverageLevelTypes
        {
            public const string CoverageAmount = "COVERAGE_AMOUNT";
            public const string Deductibility = "DEDUCTIBILITY";
        }

        public static class ProductFeatureTypes
        {
            public const string VehicleCode = "VEHICLE_CODE";
            public const string VehicleBrand = "VEHICLE_BRAND";
            public const string VehicleModel = "VEHICLE_MODEL";
            public const string VehicleYear = "VEHICLE_YEAR";
        }
    }
}
