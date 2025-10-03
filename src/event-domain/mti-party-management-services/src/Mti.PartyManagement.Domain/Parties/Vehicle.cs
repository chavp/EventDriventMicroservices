using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("Vehicles")]
    public class Vehicle : Asset
    {
        protected Vehicle() { }
        public Vehicle(string name)
        {
            Name = name;
        }

        [StringLength(10)]
        public string? Code { get; set; }

        [StringLength(300)]
        public string? Brand { get; set; }

        [StringLength(600)]
        public string? Model { get; set; }

        [StringLength(50)]
        public string? Color { get; set; }

        [StringLength(100)]
        public string? RegisterNo { get; set; }

        [StringLength(100)]
        public string? RegisterProvince { get; set; }

        public ushort? RegisterYear { get; set; }

        [StringLength(50)]
        public string? Chassis { get; set; }

        public float? Cc { get; set; }
        public float? Seat { get; set; }
        public float? Weight { get; set; }
        public float? Tonnage { get; set; }

        [StringLength(100)]
        public string? Engine { get; set; }
        public ushort? Passenger { get; set; }
    }
}
