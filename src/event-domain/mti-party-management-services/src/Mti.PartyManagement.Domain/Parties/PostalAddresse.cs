using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Domain.Parties
{
    [Table("PostalAddresses")]
    [Index(nameof(Name))]
    public class PostalAddresse : ContactMechanism
    {
        [Required, StringLength(300)]
        public string? Name { get; set; }

        /// <summary>
        /// บ้านเลขที่
        /// </summary>
        [StringLength(30)]
        public string? HouseNumber { get; set; }

        /// <summary>
        /// หมู่ที่
        /// </summary>
        [StringLength(30)]
        public string? VillageNumber { get; set; }

        /// <summary>
        /// หมู่บ้าน
        /// </summary>
        [StringLength(200)]
        public string? Village { get; set; }


        /// <summary>
        /// ตรอก / ซอย
        /// </summary>
        [StringLength(200)]
        public string? Alley { get; set; }

        [StringLength(200)]
        public string? Road { get; set; }

        /// <summary>
        /// อาคาร / ตึก
        /// </summary>
        [StringLength(200)]
        public string? Building { get; set; }

        [StringLength(30)]
        public string? Room { get; set; }

        [StringLength(30)]
        public string? Floor { get; set; }

        [StringLength(200)]
        public string? Province { get; set; }
        [StringLength(200)]
        public string? District { get; set; }
        [StringLength(200)]
        public string? SubDistrict { get; set; }
        [StringLength(50)]
        public string? ZipCode { get; set; }

        [IgnoreDataMember]
        public string DisplayName 
        {
            get
            {
                var addList = new List<string?>();
                addValue(addList, VillageNumber);
                addValue(addList, Village);
                addValue(addList, HouseNumber);
                addValue(addList, Alley);
                addValue(addList, Building);
                addValue(addList, Floor);
                addValue(addList, Room);
                addValue(addList, SubDistrict);
                addValue(addList, District);
                addValue(addList, Province); 
                addValue(addList, ZipCode);
                if(!addList.Any()) return Guid.NewGuid().ToString();

                return string.Join(" ", addList);
            }
        }

        private void addValue(List<string?> addList, string? val)
        {
            if (!string.IsNullOrEmpty(val))
            {
                addList.Add(val);
            }
        }
    }
}
