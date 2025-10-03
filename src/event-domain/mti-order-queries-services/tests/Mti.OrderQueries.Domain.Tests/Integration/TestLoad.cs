using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Mti.OrderManagement.Messaging;
using Mti.OrderQueries.Domain.OrdersWharehouse;
using Mti.OrderQueries.Persistence;
using Newtonsoft.Json;
using Xunit.Abstractions;
using Xunit.Sdk;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Mti.OrderQueries.Domain.Tests
{
    public class TestLoad
    {
        // docker run --name my-karapace -d -p 8001:8001 aivenoy/karapace

        protected readonly ITestOutputHelper _testOutputHelper;
        public TestLoad(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;

        }

        [Fact]
        public void LabLoad()
        {
            var consumerBuilder = new ConsumerBuilder<Guid, MtiOriginalOrderMessage>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "Mti.OrderQueries.Domain.Tests",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            })
            .SetValueDeserializer(new JsonDeserializer<MtiOriginalOrderMessage>())
            .SetKeyDeserializer(new JsonDeserializer<Guid>());
            var responseTopic = "dev.mti_product_management.mti_original_order.state.th.v1";
            var connectionString = "Server=localhost;TrustServerCertificate=True;User Id=postgres;Password=animalfarm888";

            var dbContextFactory = new OrderQueriesDbContextFactory(connectionString);

            using (var consumer = consumerBuilder.Build())
            {
                consumer.Subscribe(responseTopic);
                var cr = consumer.Consume();
                if (cr == null || cr.Message == null)
                {
                    return;
                }

                var orderItemState = cr.Message.Value;
                //_testOutputHelper.WriteLine(orderItemState.ToString());
                using (var db = dbContextFactory.CreateDbContext())
                using (var tran = db.Database.BeginTransaction())
                {
                    var targetOrderDim = db.OrderDims
                        .SingleOrDefault(x => x.OrderId == orderItemState.OrderId);

                    if (targetOrderDim == null)
                    {
                        targetOrderDim = new OrderDim
                        {
                            Orders_TenantId = orderItemState.Orders_TenantId,
                            OrderId = orderItemState.OrderId,
                            OrderLoanNumber = orderItemState.LoanNumber,
                            OrderSaleDate = orderItemState.SaleDate,
                            OrderNumber = orderItemState.Number,
                        };
                        db.Add(targetOrderDim);
                        db.SaveChanges();
                    }

                    var oldOrderItems = db.OrderItemFacts
                        .Include(x => x.InsuredAssetDim)
                        .Include(x => x.ApplicationDim)
                        .Include(x => x.OrderItemPartyRoleDims)
                            .ThenInclude(y => y.PartyDim)
                                .ThenInclude(x => x.ContactMechanisms)
                        .Where(x => x.OrderDimKey == targetOrderDim.Key)
                        .AsSplitQuery()
                        .ToList();
                    if (oldOrderItems.Any())
                    {
                        foreach (var oldOrderItem in oldOrderItems)
                        {
                            if (oldOrderItem.InsuredAssetDim != null)
                                db.Remove(oldOrderItem.InsuredAssetDim);
                            if (oldOrderItem.ApplicationDim != null)
                                db.Remove(oldOrderItem.ApplicationDim);
                            if (oldOrderItem.OrderItemPartyRoleDims.Any())
                            {
                                foreach ( var roleParty in oldOrderItem.OrderItemPartyRoleDims)
                                {
                                    if (roleParty.PartyDim != null)
                                    {
                                        if(roleParty.PartyDim.ContactMechanisms.Any())
                                            db.RemoveRange(roleParty.PartyDim.ContactMechanisms);
                                        db.Remove(roleParty.PartyDim);
                                    }

                                }
                                db.RemoveRange(oldOrderItem.OrderItemPartyRoleDims);
                            }
                        }
                        db.RemoveRange(oldOrderItems);
                        db.SaveChanges();
                    }

                    // new order items
                    foreach (var item in orderItemState.OrderItems)
                    {
                        var newOrderItemFact = new OrderItemFact
                        {
                            OrderDimKey = targetOrderDim.Key,

                            OrderItemId = item.OrderItemId,
                            OrderItemSeq = item.Seq,
                            OrderItemPrice = item.NetPremium,
                            OrderItemQuantity = item.Quantity,
                        };
                        db.Add(newOrderItemFact);
                        db.SaveChanges();

                        // application
                        if(item.Application != null)
                        {
                            var app = new ApplicationDim
                            {
                                ApplicationCollateralNo = item.Application.CollateralNo,
                                ApplicationCustomerInfoNo = item.Application.CustomerInfoNo,
                                ApplicationOriginalId = item.Application.OriginalId,
                                ApplicationPayPlan = item.Application.PayPlan,
                                ApplicationPolicyEffectiveDate = item.Application.PolicyEffectiveDate,
                                ApplicationPolicyExpiryDate = item.Application.PolicyExpiryDate,
                                ApplicationPolicyNumber = item.Application.PolicyNumber,
                                ApplicationPolicyPreviousNumber = item.Application.PolicyPreviousNumber,
                                ApplicationPolicyType = item.Application.PolicyType,
                                ApplicationRefDetailNo = item.Application.RefDetailNo,
                                ApplicationRefNoticeNo = item.Application.RefNoticeNo,
                                ApplicationRefQuotation = item.Application.RefQuotation,
                                ApplicationRemark = item.Application.Remark,
                                ApplicationSource = item.Application.Source,
                                ApplicationStatus = item.Application.Status,
                                ApplicationStatusMessage = item.Application.StatusMessage,
                                ApplicationSystemId = item.Application.SystemId,
                                ApplicationTransID = item.Application.TransID,
                            };
                            db.Add(app);
                            db.SaveChanges();

                            newOrderItemFact.ApplicationDimKey = app.Key;
                        }

                        // product
                        if(item.Product != null)
                        {
                            var oldPrd = db.ProductDims
                                .SingleOrDefault(x => 
                                x.ProductCode == item.Product.Code
                                && x.Products_TenantId == orderItemState.Products_TenantId);
                            if (oldPrd == null)
                            {
                                oldPrd = new ProductDim
                                {
                                    Products_TenantId = orderItemState.Products_TenantId,
                                    ProductId = item.Product.ProductId,
                                    ProductCode = item.Product.Code,
                                    ProductName = item.Product.Name,
                                    ProductCampaign = item.Product.Campaign,
                                    ProductPackage = item.Product.Package,
                                    ProductRefPolicyType = item.Product.RefPolicyType,
                                    ProductWorkshop = item.Product.Workshop,
                                };
                                db.Add(oldPrd);
                                db.SaveChanges();
                            }

                            newOrderItemFact.ProductDimKey = oldPrd.Key;
                        }

                        // insured asset
                        if (item.InsuredAsset != null)
                        {
                            var asset = new InsuredAssetDim
                            {
                                AssetId = item.InsuredAsset.AssetId,
                                AssetTypeCode = item.InsuredAsset.AssetTypeCode,
                                AssetName = item.InsuredAsset.Description,
                            };
                            if( item.InsuredAsset.Vehicle != null)
                            {
                                asset.AssetTypeCode = "VEHICLE";
                                asset.VehicleBrand = item.InsuredAsset.Vehicle.Brand;
                                asset.VehicleModel = item.InsuredAsset.Vehicle.Model;
                                asset.VehicleCc = item.InsuredAsset.Vehicle.Cc;
                                asset.VehicleEngine = item.InsuredAsset.Vehicle.Engine;
                                asset.VehicleChassis = item.InsuredAsset.Vehicle.Chassis;
                                asset.VehicleRegisterNo = item.InsuredAsset.Vehicle.RegisterNo;
                                asset.VehicleRegisterProvince = item.InsuredAsset.Vehicle.RegisterProvince;
                                asset.VehiclePassenger = item.InsuredAsset.Vehicle.Passenger;
                                asset.VehicleRegisterYear = item.InsuredAsset.Vehicle.RegisterYear;
                                asset.VehicleSeat = item.InsuredAsset.Vehicle.Seat;
                                asset.VehicleTonnage = item.InsuredAsset.Vehicle.Tonnage;
                                asset.VehicleWeight = item.InsuredAsset.Vehicle.Weight;
                            }

                            db.Add(asset);
                            db.SaveChanges();

                            newOrderItemFact.InsuredAssetDimKey = asset.Key;
                        }

                        // praties
                        if (item.Parties.Any())
                        {
                            uint seq = 0;
                            foreach (var part in item.Parties)
                            {
                                var partyDim = new PartyDim
                                {
                                    Parties_TenantId = orderItemState.Parties_TenantId,
                                    PartyId = part.PartyId,
                                    PartyTypeCode = part.PartyTypeCode,
                                    PartyTitleName = part.TitleName,
                                    PersonBirthDate = part.BirthDate,
                                    PersonCardId = part.CardId,
                                    PersonFirstName = part.FirstName,
                                    PersonLastName = part.LastName,
                                    PersonMiddleName = part.MiddleName,
                                };
                                if (part.IsOrganization.HasValue
                                    && part.IsOrganization.Value)
                                {
                                    partyDim.OrganizationName = part.FirstName;
                                }
                                db.Add(partyDim);
                                db.SaveChanges();

                                var role = new OrderItemPartyRoleDim
                                {
                                    PartyDimKey = partyDim.Key,
                                    OrderItemFactKey = newOrderItemFact.Key,
                                    OrderItemRoleTypeCode = part.RoleTypeCode,
                                    OrderItemRoleSeq = seq++
                                };
                                db.Add(role);
                                db.SaveChanges();

                                // add contact merchanism
                                foreach (var postalAddress in part.PostalAddresses)
                                {
                                    var contactMercha = new ContactMechanismDim
                                    {
                                        PartyDimKey = partyDim.Key,
                                        ContactMechanismTypeCode = "POSTAL_ADDRESS",
                                        ContactMechanismId = postalAddress.ContactMechanismId,
                                        PostalAddressAlley = postalAddress.Alley,
                                        PostalAddressBuilding = postalAddress.Building,
                                        PostalAddressDisplayName = postalAddress.DisplayName,
                                        PostalAddressDistrict = postalAddress.District,
                                        PostalAddressFloor = postalAddress.Floor,
                                        PostalAddressHouseNumber = postalAddress.HouseNumber,
                                        PostalAddressName = postalAddress.Name,
                                        PostalAddressProvince = postalAddress.Province,
                                        PostalAddressRoad = postalAddress.Road,
                                        PostalAddressRoom = postalAddress.Room,
                                        PostalAddressSubDistrict = postalAddress.SubDistrict,
                                        PostalAddressVillage = postalAddress.Village,
                                        PostalAddressVillageNumber = postalAddress.VillageNumber,
                                        PostalAddressZipCode = postalAddress.ZipCode,
                                    };

                                    db.Add(contactMercha);
                                    db.SaveChanges();


                                }
                            }
                        }
                    }

                    tran.Commit();
                }

                //consumer.Commit(cr);
                consumer.Close();
            }
        }
    }
}