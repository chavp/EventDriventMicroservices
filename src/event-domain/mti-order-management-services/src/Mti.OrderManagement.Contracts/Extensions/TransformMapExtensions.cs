using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Primitives.Result;
using Mti.OrderManagement.Contracts.Orders;
using Mti.OrderManagement.Domain.Orders.Types;

namespace Mti.OrderManagement.Contracts.Extensions
{
    public static class TransformMapExtensions
    {
        public const string MainAddress = "MAIN_ADDRESS";

        public static Result<MtiOriginalOrderResponse> Map(this Result<TranformMtiOriginalRequest> req, IReadOnlyCollection<string> orgTitleNames)
        {
            var orderTypeCode = "MTI_ORIGINAL";

            if (req.IsFailure) 
                return Result.Failure<MtiOriginalOrderResponse>(req.Errors);

            var data = req.Value;

            var respBuilder = MtiOriginalOrderResponse
                .CreateBuilder(orderTypeCode, data.SaleDate.Value, data.LoanNumber)
                .WithOrders_TenantId(null)
                .WithProducts_TenantId(null)
                .WithParties_TenantId(null)
                ;

            uint orderItemSeq = 0;
            foreach (var item in data.Items)
            {
                var parties = new List<ExtractPartyValue>();
                var orderItem = MtiOriginalOrderItemResponse
                    .CreateBuilder(orderTypeCode)
                    .WithNetPremium(item.NetPremium)
                    .WithSeq(++orderItemSeq)
                    .WithApplication(
                        orderTypeCode, app => app
                            .WithOriginalId(item.ID)
                            .WithTransID(item.TransID)
                            .WithStatus(item.Status)
                            .WithRemark(item.Remark)
                            .WithSource(item.Source)
                            .WithSystemId(item.SystemId)
                            .WithRefNoticeNo(item.RefNoticeNo)
                            .WithRefDetailNo(item.RefDetailNo)
                            .WithStatusMessage(item.StatusMessage)
                            .WithRefQuotation(item.RefQuotation)
                            .WithPayPlan(item.PayPlan)
                            .WithCollateralNo(item.CollateralNo)
                            .WithCustomerInfoNo(item.CustomerInfoNo)
                            .WithPolicyType(item.PolicyType)
                            .WithPolicyNumber(item.PolicyNo)
                            .WithPolicyPreviousNumber(item.OldPolicy)
                            .WithPolicyEffectiveDate(item.EffectiveDate)
                            .WithPolicyExpiryDate(item.ExpiryDate)
                            .ValidatePolicyDates()
                    )
                    .WithProduct(orderTypeCode, 
                        item.ProductName, 
                        item.PolicyType, 
                        p => p
                        .WithCampaign(item.Campaign)
                        .WithPackage(item.Package)
                        .WithWorkshop(item.Workshop)
                        .WithRefPolicyType(item.RefPolicyType)
                    )
                    .WithCoverage(orderTypeCode, cov => cov
                        .WithSumInsure(item.SumInsure)
                        .WithDeduct(item.Deduct)
                        .WithDamageLifePerPerson(item.DamageLifePerPerson)
                        .WithDamageLifePerTime(item.DamageLifePerTime)
                        .WithDamageInsurePerTime(item.DamageInsurePerTime)
                        .WithAccidentPerDriver(item.AccidentPerDriver)
                        .WithMedicalInsure(item.MedicalInsure)
                        .WithInsureDriver(item.InsureDriver)
                    )
                    .WithPolicy(poli => poli
                        .WithNumber(item.PolicyNo)
                    )
                    .Build();

                respBuilder.AddOrderItem(orderItem);

                var assetDescList = new List<string>();
                if (!string.IsNullOrEmpty(item.BrandName)) assetDescList.Add(item.BrandName);
                if (!string.IsNullOrEmpty(item.ModelName)) assetDescList.Add(item.ModelName);
                if (item.Yrmanuf.HasValue) assetDescList.Add(item.Yrmanuf.Value.ToString("D"));

                if (assetDescList.Any())
                {
                    orderItem.InsuredAsset = AssetValue
                        .CreateBuilder("VEHICLE")
                        .WithDescription(string.Join(",", assetDescList))
                        .WithVehicle(veh => veh
                            .WithCode(item.VehCode)
                            .WithBrand(item.BrandName)
                            .WithModel(item.ModelName)
                            .WithManufactoringYear(item.Yrmanuf)
                            .WithChassis(item.Chassis)
                            .WithColor(item.CarColour)
                            .WithRegisterProvince(item.RegProvince)
                            .WithRegisterYear(item.Yrmanuf)
                            .WithCc(item.Cc)
                            .WithSeat(item.Seat)
                            .WithWeight(item.Weight)
                            .WithTonnage(item.Toannage)
                            .WithEngine(item.Engine)
                            .WithPassenger(item.Passenger)
                        )
                        .Build();
                }

                string? ownerCardId = null;
                if (item.Owner != null
                    && item.Owner.Name != null)
                {
                    var extractOwnerParty = TransformExtensions.ExtractNameParty(orgTitleNames,
                                    item.Owner.Name);
                    // OWENER
                    if (extractOwnerParty != null
                        && extractOwnerParty.IsOrganization.HasValue)
                    {
                        ownerCardId = item.Owner.CardId;
                        extractOwnerParty.CardId = item.Owner.CardId;
                        extractOwnerParty.Nationality = item.Owner.Nationlity;
                        extractOwnerParty.BirthDate = item.Owner.BirthDate;

                        extractOwnerParty.RoleTypeCode = OrderRoleType.Insured;

                        parties.Add(extractOwnerParty);

                        var ownerAddr = item.Owner.Addr;
                        if (ownerAddr != null)
                        {
                            var extractPostalAddress = TransformExtensions
                                            .ExtractPostalAddress(ownerAddr);
                            if (extractPostalAddress != null)
                            {
                                extractPostalAddress.ContactMechanismTypeCode = MainAddress;
                                extractOwnerParty.PostalAddresses.Add(extractPostalAddress);
                            }
                        }
                    }
                }

                // INVOICE
                if (item.Invoice != null
                    && item.Invoice.Name != null)
                {
                    var extractInvoiceParty = TransformExtensions.ExtractNameParty(
                                                orgTitleNames,
                                                item.Invoice.Name);
                    if (extractInvoiceParty != null
                        && extractInvoiceParty.IsOrganization.HasValue)
                    {
                        extractInvoiceParty.CardId = item.Invoice.CardId ?? ownerCardId;
                        extractInvoiceParty.Nationality = item.Invoice.Nationlity;
                        extractInvoiceParty.BirthDate = item.Invoice.BirthDate;
                        extractInvoiceParty.RoleTypeCode = OrderRoleType.Invoice;
                        
                        parties.Add (extractInvoiceParty);
                    }
                }

                orderItem.Parties = parties;
            }

            return Result.Success(respBuilder.Build());
        }
    }
}
