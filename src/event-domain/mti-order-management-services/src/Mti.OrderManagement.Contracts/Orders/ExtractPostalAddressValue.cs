using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Contracts.Orders.Enums;
using Mti.OrderManagement.Messaging;
using Mti.PartyManagement.Messaging.Parties;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record ExtractPostalAddressValue
        : PostalAddressMessage
    {
        public EnumPatternPostalAddresses Pattern { get; set; }

        public new string? DisplayName
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
                if (!addList.Any()) return Guid.NewGuid().ToString();

                return string.Join(" ", addList);
            }
        }
        void addValue(List<string?> addList, string? val)
        {
            if (!string.IsNullOrEmpty(val))
            {
                addList.Add(val);
            }
        }
    }
}
