using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using System.Formats.Asn1;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure;
using CsvHelper;
using CsvHelper.Configuration;
using Mti.Domain.Application.Abstractions.Messaging;
using Mti.Domain.Core.Primitives.Result;
using Mti.OrderManagement.Contracts.Extensions;
using Mti.OrderManagement.Contracts.Orders;

namespace Mti.OrderManagement.Application.Orders.TransformCsvMtiOriginal
{
    public sealed class TransformCsvMtiOriginalCommandHandler
        : ICommandHandler<TransformCsvMtiOriginalCommand, Result<TransformCsvMtiOriginalResponse>>
    {
        public async Task<Result<TransformCsvMtiOriginalResponse>> Handle(TransformCsvMtiOriginalCommand command, CancellationToken cancellationToken)
        {
            IReadOnlyCollection<MtiOriginalCsvValue> mtiOriginalCsvValueList = null;
            foreach (var file in command.Request)
            {
                using var mem = new MemoryStream();
                file.CopyTo(mem);
                var bytes = mem.ToArray();
                var mtiOriCsvList = GetCSVs<MtiOriginalCsvValue>(bytes).ToList();
                foreach (var x in mtiOriCsvList)
                {
                    x.SALEDATE = x.SALEDATE.CleanNull();
                    x.CLOANNUMBER = x.CLOANNUMBER.CleanNull();
                    x.cBranchID_INV = x.cBranchID_INV.CleanNull();
                    x.cBranchName_INV = x.cBranchName_INV.CleanNull();
                    if (x.cBranchName_INV == "สำนักงานใหญ่"
                        && x.cBranchID_INV != "1")
                    {
                        x.cBranchID_INV = "1";
                    }
                }
                mtiOriginalCsvValueList = mtiOriCsvList
                    .Where(x => !string.IsNullOrEmpty(x.SALEDATE)
                                && !string.IsNullOrEmpty(x.CLOANNUMBER))
                    .ToImmutableList();
            }

            var resp = new TransformCsvMtiOriginalResponse();
            var tranformMtiOriginalRequestList = new List<TranformMtiOriginalRequest>();
            var grpTranIdKeys = mtiOriginalCsvValueList
                .GroupBy(x => new { x.SALEDATE, x.CLOANNUMBER });

            var total = grpTranIdKeys.Count();
            var limit = int.Clamp(command.Limit, 1, total);
            var totalPage = (int)Math.Ceiling((decimal)total/limit);
            var page = int.Clamp(command.Page, 1, totalPage);
            var skip = (page - 1) * limit;

            resp.Total = total;
            resp.Page = page;
            resp.Limit = limit;

            foreach (var record in grpTranIdKeys.Skip(skip).Take(limit))
            {
                DateOnly orderDate = DateOnly.ParseExact(record.Key.SALEDATE, "d/M/yyyy", CultureInfo.InvariantCulture);

                var request = new TranformMtiOriginalRequest
                {
                    SaleDate = orderDate,
                    LoanNumber = record.Key.CLOANNUMBER.CleanNull(),
                };
                var itemList = new List<TranformMtiOriginalItemRequest>();
                foreach (var item in record)
                {
                    var itemRequest = new TranformMtiOriginalItemRequest();
                    itemList.Add(itemRequest);

                    try
                    {
                        itemRequest.ID = item.ID;
                        itemRequest.TransID = item.TransID;
                        itemRequest.ProductName = item.CPRODUCTNAME.CleanNull();
                        itemRequest.PolicyType = item.POLICYTYPE.CleanNull();
                        itemRequest.Status = item.cSTATUS.CleanNull();

                        // product
                        itemRequest.RefPolicyType = item.REFPOLICYTYPE.CleanNull();
                        itemRequest.Campaign = item.CCAMPAIGN.CleanNull();
                        itemRequest.Package = item.CPACKAGE.CleanNull();
                        itemRequest.Workshop = item.CWORKSHOP.CleanNull();
                        itemRequest.NetPremium = item.NPREMIUM.ConvertDecimal();
                        itemRequest.Remark = item.REMARK.CleanNull();
                        itemRequest.RefNoticeNo = item.REFNOTICENO.CleanNull();
                        itemRequest.RefDetailNo = item.REFDETAILNO.CleanNull();
                        itemRequest.RefQuotation = item.REF_QUOTATION.CleanNull();
                        itemRequest.Source = item.SOURCE.CleanNull();
                        itemRequest.SystemId = item.SYSTEM_ID.CleanNull();
                        itemRequest.StatusMessage = item.CSTATUSMESSAGE.CleanNull();
                        itemRequest.CustomerInfoNo = item.cCustomerInfoNo.CleanNull();

                        itemRequest.EffectiveDate = item.NEFFECTIVEDATE.ConvertDate();
                        itemRequest.ExpiryDate = item.NEXPIRYDATE.ConvertDate();

                        var ownerPattern = TransformExtensions.MatchPatternNameParty(
                            item.CTITLETEXT_OWNER,
                            item.CGIVENNAME_OWNER,
                            item.CSURNAME_OWNER,
                            item.FULLNAME_OWNER);
                        if (ownerPattern != OrderManagement.Contracts.Orders.Enums.EnumPatternNames.NULL)
                        {
                            DateOnly? birthDate = item.NBIRTHDATE_OWNER.ConvertDate();
                            itemRequest.Owner = new TranformMtiOriginalItemPartyValue
                            {
                                Name = new PartyNameValue
                                {
                                    TitleText = item.CTITLETEXT_OWNER.CleanNull(),
                                    Givenname = item.CGIVENNAME_OWNER.CleanNull(),
                                    Surname = item.CSURNAME_OWNER.CleanNull(),
                                    Fullname = item.FULLNAME_OWNER.CleanNull(),
                                },
                                BirthDate = birthDate,
                                CardId = item.CCARDID_INV.CleanNull(),
                                Email = item.CEMAIL_OWNER.CleanNull(),
                                Nationlity = item.CNATIONLITY_OWNER.CleanNull(),
                                TelHome = item.CTELHOME_OWNER.CleanNull(),
                                TelMobile = item.CTELMOBILE1_OWNER.CleanNull(),
                                TelMobile1 = item.CTELMOBILE2_OWNER.CleanNull(),
                                TelOffice = item.CTELOFFICE_OWNER.CleanNull(),

                                Addr = new AddressValue
                                {
                                    No = item.CADDRNO_OWNER.CleanNull(),
                                    Moo = item.CADDRPROVINCE_OWNER.CleanNull(),
                                    Mooban = item.CADDRMOOBAN_OWNER.CleanNull(),
                                    Ampur = item.CADDRAMPUR_OWNER.CleanNull(),
                                    Tumbol = item.CADDRTUMBOL_OWNER.CleanNull(),
                                    Province = item.CADDRPROVINCE_OWNER.CleanNull(),
                                    Building = item.CADDRBUILDING_OWNER.CleanNull(),
                                    Floor = item.CADDRFLOOR_OWNER.CleanNull(),
                                    Road = item.CADDRROAD_OWNER.CleanNull(),
                                    Room = item.CADDRROOM_OWNER.CleanNull(),
                                    Soi = item.CADDRSOI_OWNER.CleanNull(),
                                    Zipcode = item.CADDRZIPCODE_OWNER.CleanNull(),
                                    Line1 = item.CADDRLINE1_OWNER.CleanNull(),
                                    Line2 = item.CADDRLINE2_OWNER.CleanNull(),
                                    Line3 = item.CADDRLINE3_OWNER.CleanNull(),
                                    Line4 = item.CADDRLINE4_OWNER.CleanNull(),
                                },
                            };
                        }

                        var invPattern = TransformExtensions.MatchPatternNameParty(
                            item.CTITLETEXT_INV,
                            item.CGIVENNAME_INV,
                            item.CSURNAME_INV,
                            item.FULLNAME_INV);
                        if (invPattern != OrderManagement.Contracts.Orders.Enums.EnumPatternNames.NULL)
                        {
                            DateOnly? birthDate = item.NBIRTHDATE_INV.ConvertDate();
                            itemRequest.Invoice = new TranformMtiOriginalItemPartyValue
                            {
                                Name = new PartyNameValue
                                {
                                    TitleText = item.CTITLETEXT_INV.CleanNull(),
                                    Givenname = item.CGIVENNAME_INV.CleanNull(),
                                    Surname = item.CSURNAME_INV.CleanNull(),
                                    Fullname = item.FULLNAME_INV.CleanNull(),
                                },
                                BirthDate = birthDate,
                                CardId = item.CCARDID_INV.CleanNull(),
                                Nationlity = item.CNATIONLITY_INV.CleanNull(),

                                BranchId = item.cBranchID_INV.CleanNull(),
                                BranchName = item.cBranchName_INV.CleanNull()
                            };
                        }

                        // coverage
                        itemRequest.SumInsure = item.SUMINSURE;
                        itemRequest.Deduct = item.Deduct.ConvertDecimal();
                        itemRequest.DamageLifePerPerson = item.DamageLifePerPerson.ConvertDecimal();
                        itemRequest.DamageLifePerTime = item.DamageLifePerTime.ConvertDecimal();
                        itemRequest.DamageInsurePerTime = item.DamageInsurePerTime.ConvertDecimal();
                        itemRequest.AccidentPerDriver = item.AccidentPerDriver.ConvertDecimal();
                        itemRequest.MedicalInsure = item.MedicalInsure.ConvertDecimal();
                        itemRequest.InsureDriver = item.InsureDriver.ConvertDecimal();

                        // vehicle
                        itemRequest.VehCode = item.CVEHCODE.CleanNull();
                        itemRequest.BrandName = item.BRANDNAME.CleanNull();
                        itemRequest.ModelName = item.MODELNAME.CleanNull();
                        itemRequest.Yrmanuf = item.NYRMANUF.ConvertUshort();
                        itemRequest.RegNo = item.CREGNO.CleanNull();
                        itemRequest.Engine = item.CENGINE.CleanNull();
                        itemRequest.Chassis = item.CCHASSIS.CleanNull();
                        itemRequest.RegProvince = item.CREGPROVINCE.CleanNull();
                        itemRequest.Cc = item.NCC.ConvertFloat();
                        itemRequest.Seat = item.NSEAT.ConvertUshort();
                        itemRequest.Weight = item.NWEIGHT.ConvertFloat();
                        itemRequest.Toannage = item.NTOANNAGE.ConvertFloat();
                        itemRequest.Passenger = item.NPASSENGER.ConvertUshort();
                        itemRequest.PayPlan = item.cPayPlan.CleanNull();
                        itemRequest.CollateralNo = item.cCollateralNo.CleanNull();
                        itemRequest.CarColour = item.cCarColour.CleanNull();
                    }
                    catch(Exception ex)
                    {
                        itemRequest.Errors.Add($"ID:{item.ID}, ERROR: {ex.Message}");
                    }
                }
                request.Items = itemList;
                tranformMtiOriginalRequestList.Add(request);
            }

            resp.Data = tranformMtiOriginalRequestList.AsReadOnly();
            return Result.Success(resp);
        }

        public static IReadOnlyCollection<T> GetCSVs<T>(byte[] bytes)
        {

            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            //var encode = new UTF8Encoding(true);
            var encode = Encoding.GetEncoding("windows-874");
            //var encode = Encoding.UTF8;
            var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {

            };
            using (var mem = new MemoryStream(bytes))
            using (var reader = new StreamReader(mem, encode))
            using (var csv = new CsvReader(reader, configuration))
            {
                return csv.GetRecords<T>().ToImmutableList();
            }
        }

    }
}
