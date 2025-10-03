using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Contracts.Orders;
using Mti.OrderManagement.Contracts.Orders.Enums;

namespace Mti.OrderManagement.Contracts.Extensions
{
    public static class TransformExtensions
    {
        public const string DateFormat = "d/M/yyyy";
        public const string DateFormat2 = "ddMMyyyy";
        public static EnumPatternNames MatchPatternNameParty(string? ctitletext
            , string? cgivenname
            , string? csurname
            , string? fullname)
        {
            var ctitletext_owner = ctitletext;
            var cgivenname_owner = cgivenname;
            var csurname_owner = csurname;
            var fullname_owner = fullname;

            var pattern = EnumPatternNames.UNKNOWN;
            if (!string.IsNullOrEmpty(ctitletext_owner)
                && !string.IsNullOrEmpty(cgivenname_owner)
                && !string.IsNullOrEmpty(csurname_owner))
            {
                pattern = EnumPatternNames.GOOD;
            }
            else if (!string.IsNullOrEmpty(ctitletext_owner)
                && string.IsNullOrEmpty(cgivenname_owner)
                && string.IsNullOrEmpty(csurname_owner)
                && !string.IsNullOrEmpty(fullname_owner))
            {
                pattern = EnumPatternNames.TITLE_FULLNAME;
            }
            else if (!string.IsNullOrEmpty(ctitletext_owner)
                && !string.IsNullOrEmpty(cgivenname_owner)
                && string.IsNullOrEmpty(csurname_owner))
            {
                pattern = EnumPatternNames.TITLE_FIRSTNAME;
            }
            else if (string.IsNullOrEmpty(ctitletext_owner)
                && !string.IsNullOrEmpty(cgivenname_owner)
                && !string.IsNullOrEmpty(csurname_owner))
            {
                pattern = EnumPatternNames.FIRSTNAME_LASTNAME;
            }
            else if (string.IsNullOrEmpty(ctitletext_owner)
                && !string.IsNullOrEmpty(cgivenname_owner)
                && string.IsNullOrEmpty(csurname_owner))
            {
                pattern = EnumPatternNames.FIRSTNAME;
            }
            else if (string.IsNullOrEmpty(ctitletext_owner)
                && string.IsNullOrEmpty(cgivenname_owner)
                && string.IsNullOrEmpty(csurname_owner)
                && !string.IsNullOrEmpty(fullname_owner))
            {
                pattern = EnumPatternNames.FULLNAME;
            }
            else if (string.IsNullOrEmpty(ctitletext_owner)
                && string.IsNullOrEmpty(cgivenname_owner)
                && string.IsNullOrEmpty(csurname_owner)
                && string.IsNullOrEmpty(fullname_owner))
            {
                pattern = EnumPatternNames.NULL;
            }
            return pattern;
        }
        public static string MatchPatternPostalAddress()
        {
            var pattern = "UNKNOWN";
            //CADDRNO_OWNER
            //CADDRMOO_OWNER
            //CADDRFLOOR_OWNER
            //CADDRROOM_OWNER
            //CADDRMOOBAN_OWNER
            //CADDRBUILDING_OWNER
            //CADDRSOI_OWNER
            //CADDRROAD_OWNER
            //CADDRTUMBOL_OWNER
            //CADDRAMPUR_OWNER
            //CADDRPROVINCE_OWNER
            //CADDRZIPCODE_OWNER
            //CADDRLINE1_OWNER
            //CADDRLINE2_OWNER
            //CADDRLINE3_OWNER
            //CADDRLINE4_OWNER
            //CADDRLINE5_OWNER
            return pattern;
        }

        public static ExtractPartyValue? ExtractNameParty(
            IReadOnlyCollection<string> orgTitleNames
            , PartyNameValue name)
        {
            var (ctitletext, cgivenname, csurname, fullname) 
                = (name.TitleText, name.Givenname, name.Surname, name.Fullname);

            var pattern = MatchPatternNameParty(ctitletext, cgivenname, csurname, fullname);
            ExtractPartyValue? result = null;
            switch (pattern)
            {
                case EnumPatternNames.GOOD:
                    result = ExtractPatternNameGood(orgTitleNames, ctitletext, cgivenname, csurname);
                    break;
                case EnumPatternNames.TITLE_FULLNAME:
                    result = ExtractPatternNameTitleFullName(orgTitleNames, ctitletext, fullname);
                    break;
                case EnumPatternNames.TITLE_FIRSTNAME:
                    result = ExtractPatternNameTitleFirstName(orgTitleNames, ctitletext, cgivenname);
                    break;
                case EnumPatternNames.FIRSTNAME_LASTNAME:
                    result = ExtractPatternNameFirstNameLastName(orgTitleNames, cgivenname, csurname);
                    break;
                case EnumPatternNames.FIRSTNAME:
                    result = ExtractPatternNameFirstName(orgTitleNames, cgivenname);
                    break;
                case EnumPatternNames.FULLNAME:
                    result = ExtractPatternNameFullName(orgTitleNames, fullname);
                    break;
                case EnumPatternNames.NULL:
                    break;
                default:
                    throw new InvalidOperationException($"Unknown party pattern: {pattern}");
            }

            return result;
        }

        public static ExtractPartyValue ExtractPatternNameGood(
            IReadOnlyCollection<string> orgTitleNames,
            string titleText,
            string givenname,
            string surname)
        {
            bool? isOrganization = null;
            string? title = null;
            string? firstName = null;
            string? middleName = null;
            string? lastName = null;

            isOrganization = orgTitleNames.Contains(titleText);
            title = titleText;
            firstName = givenname;
            lastName = surname;

            if (!isOrganization.HasValue)
            {
                foreach (var orgTitleName in orgTitleNames)
                {
                    if (surname.Contains(orgTitleName))
                    {
                        isOrganization = true;
                        title = orgTitleName;
                        (firstName, middleName, lastName) = SplitName(givenname.Trim());
                        break;
                    }
                }
            }

            return new ExtractPartyValue
            {
                Pattern = EnumPatternNames.GOOD,
                IsOrganization = isOrganization,
                TitleName = title,
                FirstName = firstName,
                MiddleName = middleName,
                LastName = lastName,
            };
        }
        public static ExtractPartyValue ExtractPatternNameTitleFullName(
            IReadOnlyCollection<string> orgTitleNames,
            string titleText,
            string fullname)
        {
            bool? isOrganization = null;
            string? title = titleText;
            string? firstName = null;
            string? middleName = null;
            string? lastName = null;

            isOrganization = orgTitleNames.Contains(titleText);
            var fullname_notitle = fullname?.Replace(titleText, "").Trim();
            (firstName, middleName, lastName) = SplitName(fullname_notitle);
            if (string.IsNullOrEmpty(firstName)
                 && string.IsNullOrEmpty(middleName)
                 && string.IsNullOrEmpty(lastName))
            {
                firstName = fullname_notitle;
            }

            return new ExtractPartyValue
            {
                Pattern = EnumPatternNames.TITLE_FULLNAME,
                IsOrganization = isOrganization,
                TitleName = title,
                FirstName = firstName,
                MiddleName = middleName,
                LastName = lastName,
            };
        }
        public static ExtractPartyValue ExtractPatternNameTitleFirstName(
            IReadOnlyCollection<string> orgTitleNames,
            string titleText,
            string givenname)
        {
            bool? isOrganization = null;
            string? title = titleText;
            string? firstName = null;
            string? middleName = null;
            string? lastName = null;

            title = titleText;
            isOrganization = orgTitleNames.Contains(titleText);
            var firstname_notitle = givenname?.Replace(titleText, "").Trim();
            firstName = firstname_notitle;

            if (!isOrganization.Value)
            {
                (firstName, middleName, lastName) = SplitName(firstname_notitle);
            }

            return new ExtractPartyValue
            {
                Pattern = EnumPatternNames.TITLE_FIRSTNAME,
                IsOrganization = isOrganization,
                TitleName = title,
                FirstName = firstName,
                MiddleName = middleName,
                LastName = lastName,
            };
        }
        public static ExtractPartyValue ExtractPatternNameFirstNameLastName(
            IReadOnlyCollection<string> orgTitleNames,
            string givenname,
            string surname)
        {
            bool? isOrganization = null;
            string? title = null;
            string? firstName = null;
            string? middleName = null;
            string? lastName = null;

            foreach (var orgTitleName in orgTitleNames)
            {
                if (givenname.Contains(orgTitleName))
                {
                    isOrganization = true;
                    var name = givenname.Replace(orgTitleName, "").Trim();
                    title = orgTitleName;
                    (firstName, middleName, lastName) = SplitName(name);
                    break;
                }

            }

            var joinedName = $"{givenname} {surname}";
            if (!isOrganization.HasValue)
            {
                if (joinedName.Contains("จำกัด")
                    || joinedName.Contains("มหาชน"))
                {
                    isOrganization = true;
                    firstName = joinedName;
                }
                else
                {
                    isOrganization = false;
                    (firstName, middleName, lastName) = SplitName(joinedName);
                }
            }

            return new ExtractPartyValue
            {
                Pattern = EnumPatternNames.FIRSTNAME_LASTNAME,
                IsOrganization = isOrganization,
                TitleName = title,
                FirstName = firstName,
                MiddleName = middleName,
                LastName = lastName,
            };
        }
        public static ExtractPartyValue ExtractPatternNameFirstName(
            IReadOnlyCollection<string> orgTitleNames,
            string givenname)
        {
            bool? isOrganization = null;
            string? title = null;
            string? firstName = null;
            string? middleName = null;
            string? lastName = null;

            foreach (var orgTitleName in orgTitleNames)
            {
                if (givenname.Contains(orgTitleName))
                {
                    isOrganization = true;
                    firstName = givenname.Replace(orgTitleName, "").Trim();
                    title = orgTitleName;
                    break;
                }
                if (!isOrganization.HasValue)
                {
                    if (givenname.Contains("จำกัด")
                        || givenname.Contains("มหาชน"))
                    {
                        isOrganization = true;
                        firstName = givenname;
                        break;
                    }
                }
            }
            if (!isOrganization.HasValue)
            {
                (title, firstName, middleName, lastName) = SplitTitleName(givenname);
            }

            return new ExtractPartyValue
            {
                Pattern = EnumPatternNames.FIRSTNAME,
                IsOrganization = isOrganization,
                TitleName = title,
                FirstName = firstName,
                MiddleName = middleName,
                LastName = lastName,
            };
        }
        public static ExtractPartyValue ExtractPatternNameFullName(
            IReadOnlyCollection<string> orgTitleNames,
            string fullname)
        {
            bool? isOrganization = null;
            string? title = null;
            string? firstName = null;
            string? middleName = null;
            string? lastName = null;

            foreach (var orgTitleName in orgTitleNames)
            {
                if (fullname.Contains(orgTitleName))
                {
                    isOrganization = true;
                    var name = fullname.Replace(orgTitleName, "").Trim();
                    title = orgTitleName;
                    firstName = name;
                    break;
                }
            }
            if (!isOrganization.HasValue)
            {
                if (fullname.Contains("จำกัด")
                    || fullname.Contains("มหาชน"))
                {
                    isOrganization = true;
                    firstName = fullname;
                }
                else
                {
                    (title, firstName, middleName, lastName) = SplitTitleName(fullname);
                    if (!string.IsNullOrEmpty(title))
                    {
                        isOrganization = false;
                    }
                }
            }

            return new ExtractPartyValue
            {
                Pattern = EnumPatternNames.FULLNAME,
                IsOrganization = isOrganization,
                TitleName = title,
                FirstName = firstName,
                MiddleName = middleName,
                LastName = lastName,
            };
        }

        public static ExtractPostalAddressValue? ExtractPostalAddress(AddressValue addressValue)
        {
            ExtractPostalAddressValue? result = null;
            if (!string.IsNullOrEmpty(addressValue.Province))
            {
                result = new ExtractPostalAddressValue
                {
                    Pattern = EnumPatternPostalAddresses.Normal,
                    HouseNumber = addressValue.No,
                    VillageNumber = addressValue.Moo,
                    Floor = addressValue.Floor,
                    Room = addressValue.Room,
                    Village = addressValue.Mooban,
                    Building = addressValue.Building,
                    Alley = addressValue.Soi,
                    Road = addressValue.Room,

                    Province = addressValue.Province,
                    District = addressValue.Ampur,
                    SubDistrict = addressValue.Tumbol,

                    ZipCode = addressValue.Zipcode,
                };
                result.Name = result.DisplayName;
            }
            else
            {
                var addrlines = new List<string>();
                if (!string.IsNullOrEmpty(addressValue.Line1)) addrlines.Add(addressValue.Line1);
                if (!string.IsNullOrEmpty(addressValue.Line2)) addrlines.Add(addressValue.Line2);
                if (!string.IsNullOrEmpty(addressValue.Line3)) addrlines.Add(addressValue.Line3);
                if (!string.IsNullOrEmpty(addressValue.Line4)) addrlines.Add(addressValue.Line4);
                if (addrlines.Any())
                {
                    result = new ExtractPostalAddressValue
                    {
                        Pattern = EnumPatternPostalAddresses.Line
                    };
                    var addrline = string.Join(" ", addrlines);
                    var adds = addrline
                        .Split(' ', StringSplitOptions.RemoveEmptyEntries)
                        .ToList();
                    addrline = string.Join(" ", adds);

                    if (adds.Any())
                    {
                        adds.Reverse();

                        int zipCode = 0;
                        if (int.TryParse(adds[0], out zipCode))
                            result.ZipCode = adds[0];

                        result.Province = adds[1]
                            .Replace("จ.", "").Replace("จังหวัด", "");
                        result.District = adds[2]
                            .Replace("อ.", "").Replace("อำเภอ", "")
                            .Replace("เขต", "");
                        result.SubDistrict = adds[3]
                            .Replace("ต.", "").Replace("ตำบล", "")
                            .Replace("แขวง", "");

                        var addNames = adds.Skip(4).ToList();
                        addNames.Reverse();

                        result.Village = string.Join(" ", addNames);

                        result.Name = result.DisplayName;
                    }
                }
            }
            return result;
        }

        public static (string?, string?, string?) SplitName(this string? name)
        {
            var names = name?.Split(' ', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (names.Length == 1)
            {
                return (names[0], null, null);
            }
            else if (names.Length == 2)
            {
                var firstName = names[0];
                var lastName = names[1];
                return (firstName, null, lastName);
            }
            else if (names.Length == 3)
            {
                var firstName = names[0];
                var middleName = names[1];
                var lastName = names[2];
                return (firstName, middleName, lastName);
            }
            return (null, null, null);
        }
        public static (string?, string?, string?, string?) SplitTitleName(this string? name)
        {
            var names = name?.Split(' ', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (names.Length == 3)
            {
                return (names[0], names[1], null, names[2]);
            }
            else if (names.Length == 4)
            {
                var titleName = names[0];
                var firstName = names[1];
                var middleName = names[2];
                var lastName = names[3];
                return (titleName, firstName, middleName, lastName);
            }
            return (null, null, null, null);
        }
        public static string? CleanNull(this string? data)
        {
            if (string.IsNullOrEmpty(data))
                return null;
            data = data.Trim()
                .Replace("NULL", "")
                .Replace("-", "")
                .Replace("UNKNOWN", "")
                ;
            if (string.IsNullOrEmpty(data)) return null;
            return data;
        }
        public static string? GenCode(this string? data)
        {
            if (string.IsNullOrEmpty(data))
                return null;
            data = data.Trim();
            var codes = data.Split(" ", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (!codes.Any()) return null;

            var code = "#" + string.Join("_", codes).ToUpperInvariant();
            return code;
        }
        public static string ComputeSha256Hash(this string rawData)
        {
            // Create a SHA256
            using (var sha256Hash = SHA256.Create())
            {
                // ComputeHash - returns byte array
                byte[] bytes = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(rawData));

                // Convert byte array to a string
                var builder = new StringBuilder();
                for (int i = 0; i < bytes.Length; i++)
                {
                    builder.Append(bytes[i].ToString("x2"));
                }
                return builder.ToString();
            }
        }

        public static decimal ConvertDecimal(this string? data)
        {
            var amtSt = CleanNull(data);
            if (string.IsNullOrEmpty(amtSt)) return 0;
            amtSt = amtSt.Replace(" ", "");
            var amt = decimal.Parse(amtSt);
            return amt;
        }
        public static ushort ConvertUshort(this string? data)
        {
            var amtSt = CleanNull(data);
            if (string.IsNullOrEmpty(amtSt)) return 0;
            amtSt = amtSt.Replace(" ", "");
            var amt = ushort.Parse(amtSt);
            return amt;
        }
        public static float ConvertFloat(this string? data)
        {
            var amtSt = CleanNull(data);
            if (string.IsNullOrEmpty(amtSt)) return 0;
            amtSt = amtSt.Replace(" ", "");
            var amt = float.Parse(amtSt);
            return amt;
        }

        public static DateOnly? ConvertDate(this string? data)
        {
            data = data.CleanNull();
            DateOnly? date = null;
            if (!string.IsNullOrEmpty(data))
            {
                date = DateOnly.ParseExact(data, DateFormat, CultureInfo.InvariantCulture);
            }
            return date;
        }
    }
}
