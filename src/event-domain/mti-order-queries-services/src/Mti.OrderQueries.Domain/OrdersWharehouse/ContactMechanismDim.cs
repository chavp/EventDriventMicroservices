using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderQueries.Domain.OrdersWharehouse
{
    public class ContactMechanismDim : StarModel
    {
        public Guid? ContactMechanismId { get; set; }

        [Required]
        public Guid? PartyDimKey { get; set; }
        [ForeignKey(nameof(PartyDimKey))]
        public PartyDim? PartyDim { get; set; }

        [Required, StringLength(300)]
        public string? ContactMechanismTypeCode { get; set; }

        // PostalAddress
        [Required, StringLength(300)]
        public string? PostalAddressName { get; set; }

        /// <summary>
        /// บ้านเลขที่
        /// </summary>
        [StringLength(30)]
        public string? PostalAddressHouseNumber { get; set; }

        /// <summary>
        /// หมู่ที่
        /// </summary>
        [StringLength(30)]
        public string? PostalAddressVillageNumber { get; set; }

        /// <summary>
        /// หมู่บ้าน
        /// </summary>
        [StringLength(200)]
        public string? PostalAddressVillage { get; set; }


        /// <summary>
        /// ตรอก / ซอย
        /// </summary>
        [StringLength(200)]
        public string? PostalAddressAlley { get; set; }

        [StringLength(200)]
        public string? PostalAddressRoad { get; set; }

        /// <summary>
        /// อาคาร / ตึก
        /// </summary>
        [StringLength(200)]
        public string? PostalAddressBuilding { get; set; }

        [StringLength(30)]
        public string? PostalAddressRoom { get; set; }

        [StringLength(30)]
        public string? PostalAddressFloor { get; set; }

        [StringLength(200)]
        public string? PostalAddressProvince { get; set; }

        [StringLength(200)]
        public string? PostalAddressDistrict { get; set; }

        [StringLength(200)]
        public string? PostalAddressSubDistrict { get; set; }

        [StringLength(50)]
        public string? PostalAddressZipCode { get; set; }

        [StringLength(3000)]
        public string? PostalAddressDisplayName { get; set; }
    }
}
