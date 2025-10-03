using System.Threading.Channels;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Mti.Domain.Application.Abstractions.Messaging;
using Mti.Domain.Core.Errors;
using Mti.Domain.Core.Primitives.Result;
using Mti.Domain.Infrastructure.Extensions;
using Mti.OrderManagement.Application.Orders.SavePartiesByOrder;
using Mti.OrderManagement.Contracts.Extensions;
using Mti.OrderManagement.Contracts.Orders;
using Mti.OrderManagement.Domain.Orders;
using Mti.OrderManagement.Domain.Orders.Types;
using Mti.OrderManagement.Persistence;
using Mti.OrderManagement.Persistence.Repositories;
using Mti.PartyManagement.Messaging;
using Mti.ProductManagement.Messaging.Products.Commands;
using Newtonsoft.Json;

namespace Mti.OrderManagement.Application.Orders.LoadMtiOriginal
{
    public sealed class SaveMtiOriginalCommandHandler
        : ICommandHandler<SaveMtiOriginalCommand, Result<MtiOriginalOrderResponse>>
    {
        private readonly ILogger _logger;
        private readonly IDbContextFactory<OrdersContext> _dbContextFactory;
        private readonly IPartyRepository _partiesRepository;
        //private readonly Channel<SavePartiesByOrderRequest> _savePartiesByOrderChannel;
        private readonly Channel<SaveProductsByOrderRequest> _saveProductsByOrderRequestChannel;
        private readonly Channel<MtiOriginalOrderResponse> _mtiOriginalOrderResponseChannel;

        private readonly SavePartiesByOrderProducer _savePartiesByOrderProducer;
        public SaveMtiOriginalCommandHandler(
            ILogger logger,
            IDbContextFactory<OrdersContext> dbContextFactory,
            IPartyRepository partiesRepository,
            //Channel<SavePartiesByOrderRequest> savePartiesByOrderChannel,
            Channel<SaveProductsByOrderRequest> saveProductsByOrderRequestChannel,
            Channel<MtiOriginalOrderResponse> mtiOriginalOrderResponseChannel,
            SavePartiesByOrderProducer savePartiesByOrderProducer)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            //if(savePartiesByOrderChannel == null)
            //    throw new ArgumentNullException(nameof(savePartiesByOrderChannel));
            if (dbContextFactory == null)
                throw new ArgumentNullException(nameof(dbContextFactory));
            if (partiesRepository == null)
                throw new ArgumentNullException(nameof(partiesRepository));

            _dbContextFactory = dbContextFactory;
            _partiesRepository = partiesRepository;
            //_savePartiesByOrderChannel = savePartiesByOrderChannel;

            _savePartiesByOrderProducer = savePartiesByOrderProducer;

            _saveProductsByOrderRequestChannel = saveProductsByOrderRequestChannel;
            _mtiOriginalOrderResponseChannel = mtiOriginalOrderResponseChannel;
        }

        public async Task<Result<MtiOriginalOrderResponse>> Handle(SaveMtiOriginalCommand request, CancellationToken cancellationToken)
        {
            _logger.LogDebug($"Handle = {JsonConvert.SerializeObject(request)}");

            var req = request.Request;

            // save Orders
            IReadOnlyCollection<string> orgTitleNames = _partiesRepository.GetOrganizationTitles();

            var reqResult = Result.Create(request.Request, [DomainErrors.General.UnProcessableRequest]);
            if (reqResult.IsFailure) return Result.Failure<MtiOriginalOrderResponse>(reqResult.Errors);

            var mtiSaleOrderResult = reqResult.Map(orgTitleNames);
            if (mtiSaleOrderResult.IsFailure) return mtiSaleOrderResult;

            var mtiSaleOrder = mtiSaleOrderResult.Value;

            // save order
            var savePartiesByOrderRequest = await saveOrderAsync(mtiSaleOrder, orgTitleNames, cancellationToken);

            var saveTasks = new List<Task>();

            // save parties
            //_savePartiesByOrderChannel.Writer.TryWrite(savePartiesByOrderRequest);
            // sync request/response 
            var saveProtyTask = _savePartiesByOrderProducer.PrcessRequestAsync(savePartiesByOrderRequest, cancellationToken);
            saveTasks.Add(saveProtyTask);

            // save Products
            // async request/response
            var saveProductRequest = await getSaveProductByOrderRequestAsync(mtiSaleOrder, cancellationToken);
            _saveProductsByOrderRequestChannel.Writer.TryWrite(saveProductRequest);

            // save Policies

            await Task.WhenAll(saveTasks);

            // Raise state order changed
            _logger.LogDebug($"{JsonConvert.SerializeObject(mtiSaleOrder)}");
            await _mtiOriginalOrderResponseChannel.Writer.WriteAsync(mtiSaleOrder, cancellationToken);

            return Result.Success(mtiSaleOrder);
        }

        private async Task<SaveProductsByOrderRequest> getSaveProductByOrderRequestAsync(MtiOriginalOrderResponse record
            , CancellationToken cancellationToken)
        {
            using (var ordDb = _dbContextFactory.CreateDbContext())
            using (var tran = ordDb.Database.BeginTransaction()) 
            {
                var saleOrder = await ordDb.Orders.OfType<MtiOriginalSalesOrder>()
                       .Include(x => x.Items)
                       .SingleAsync(x => x.SaleDate == record.SaleDate
                                   && x.LoanNumber == record.LoanNumber, cancellationToken);

                var saveProductByOrderRequest = new SaveProductsByOrderRequest(saleOrder.Id.Value)
                {
                    OrderNumber = saleOrder.OrderNumber,
                };

                var saveProductByOrderItemRequests = new List<SaveProductByOrderItemRequest>();

                foreach (var item in record.OrderItems)
                {
                    var saleItem = saleOrder.Items.OfType<MtiOriginalSalesOrderItem>()
                        .Single(x => x.Id == item.OrderItemId);

                    var productName = saleItem.GetProductName();
                    var productCode = productName.GenCode();

                    // order item main product
                    var orderItemReq = new SaveProductByOrderItemRequest(saleItem.Id.Value)
                    {
                        OrderItemSeq = saleItem.Seq,
                    };
                    orderItemReq.Product = new ProductRequest(productCode, productName);
                    saveProductByOrderItemRequests.Add(orderItemReq);

                    // pick Coverages
                    //addCoverageAmount(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    saleItem.SumInsure, 
                    //    CoverageTypes.SumInsured,
                    //    CoverageLevelTypes.CoverageAmount,
                    //    CoverageLevelBasises.PerIncident);

                    //addCoverageAmount(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    saleItem.Deduct,
                    //    CoverageTypes.Deductible,
                    //    CoverageLevelTypes.Deductibility,
                    //    CoverageLevelBasises.PerIncident
                    //);

                    //addCoverageAmount(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    saleItem.DamageLifePerPerson,
                    //    CoverageTypes.DamageLife,
                    //    CoverageLevelTypes.CoverageAmount,
                    //    CoverageLevelBasises.PerPerson);

                    //addCoverageAmount(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    saleItem.DamageLifePerTime,
                    //    CoverageTypes.DamageLife,
                    //    CoverageLevelTypes.CoverageAmount,
                    //    CoverageLevelBasises.PerTime);

                    //addCoverageAmount(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    saleItem.DamageInsurePerTime,
                    //    CoverageTypes.DamageInsure,
                    //    CoverageLevelTypes.CoverageAmount,
                    //    CoverageLevelBasises.PerTime);

                    //addCoverageAmount(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    saleItem.AccidentPerDriver,
                    //    CoverageTypes.Accident,
                    //    CoverageLevelTypes.CoverageAmount,
                    //    CoverageLevelBasises.PerDriver);

                    //addCoverageAmount(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    saleItem.MedicalInsure,
                    //    CoverageTypes.MedicalInsure,
                    //    CoverageLevelTypes.CoverageAmount,
                    //    CoverageLevelBasises.PerTime);

                    //addCoverageAmount(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    saleItem.InsureDriver,
                    //    CoverageTypes.InsureDriver,
                    //    CoverageLevelTypes.CoverageAmount,
                    //    CoverageLevelBasises.PerTime);

                    // pick product features
                    //addProductFeature(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    ProductFeatureTypes.VehicleCode, saleItem.VehicleCode);
                    //addProductFeature(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    ProductFeatureTypes.VehicleBrand, saleItem.VehicleBrand);
                    //addProductFeature(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    ProductFeatureTypes.VehicleModel, saleItem.VehicleModel);
                    //if (saleItem.VehicleManufactoringYear > 0)
                    //{
                    //    addProductFeature(ordDb, saveProductByOrderItemRequests,
                    //    saleOrder, saleItem.Id.Value,
                    //    ProductFeatureTypes.VehicleYear, saleItem.VehicleManufactoringYear.ToString());
                    //}

                }

                saveProductByOrderRequest.OrderItems
                    = saveProductByOrderItemRequests.AsReadOnly();

                await tran.CommitAsync(cancellationToken);

                return saveProductByOrderRequest;
            }
        }

        public void addCoverageAmount(
            OrdersContext ordDb,
            List<SaveProductByOrderItemRequest> saveProductByOrderItemRequests,
            Order saleOrder,
            Guid forOrderItem,
            decimal amt,
            string coverageTypeCode,
            string coverageAmountTypeCode,
            string coverageLevelBasisCode)
        {
            if (amt > 0)
            {
                var orderItem = new SalesOrderItem()
                {
                    OrderedForId = forOrderItem
                };
                ordDb.Add(orderItem);
                saleOrder.AddItem(orderItem);
                ordDb.SaveChanges();
                var savePrdRequest = new SaveProductByOrderItemRequest(orderItem.Id.Value);
                savePrdRequest.Coverage = new CoverageRequest(
                    coverageTypeCode,
                    coverageAmountTypeCode,
                    coverageLevelBasisCode,
                    amt, null, null, null);
                saveProductByOrderItemRequests.Add(savePrdRequest);
            }
        }

        public void addProductFeature(
            OrdersContext ordDb,
            List<SaveProductByOrderItemRequest> saveProductByOrderItemRequests,
            Order saleOrder,
            Guid forOrderItem,
            string productFeatureTypeCode,
            string productFeatureName)
        {
            var code = productFeatureName.CleanNull();
            if (!string.IsNullOrEmpty(code))
            {
                var orderItem = new SalesOrderItem()
                {
                    OrderedForId = forOrderItem
                };
                ordDb.Add(orderItem);
                saleOrder.AddItem(orderItem);
                ordDb.SaveChanges();
                var savePrdRequest = new SaveProductByOrderItemRequest(orderItem.Id.Value);
                savePrdRequest.ProductFeature = new ProductFeatureRequest(
                    productFeatureTypeCode,
                    code,
                    productFeatureName
                    );
                saveProductByOrderItemRequests.Add(savePrdRequest);
            }
        }

        private async Task<SavePartiesByOrderRequest> saveOrderAsync(
            MtiOriginalOrderResponse mtiSaleOrder,
            IReadOnlyCollection<string> orgTitleNames, 
            CancellationToken cancellationToken)
        {

            using (var ordDb = _dbContextFactory.CreateDbContext())
            using (var tran = ordDb.Database.BeginTransaction())
            {
                var deleteParties = new List<DeletePartyRequest>();
                var deleteAssets = new List<DeleteAssetRequest>();

                var saveOrder = ordDb.Orders.OfType<MtiOriginalSalesOrder>()
                            .Include(x => x.Items)
                                .ThenInclude(x => x.Roles)
                        //.ThenInclude(y => y.InsuredAsset)
                        .SingleOrDefault(x => x.OrderNumber == mtiSaleOrder.Number);
                if (saveOrder != null)
                {
                    // Clean up existing order items and roles
                    if (saveOrder.Items.Any())
                    {
                        foreach (var item in saveOrder.Items.OfType<MtiOriginalSalesOrderItem>())
                        {
                            if (item.InsuredAssetId.HasValue)
                            {
                                ordDb.Find<InsuredAsset>(item.InsuredAssetId);
                                if (item.InsuredAsset.Parties_AssetId.HasValue)
                                {
                                    // delete asset
                                    deleteAssets.Add(new DeleteAssetRequest(item.InsuredAsset.Parties_AssetId.Value));
                                }
                                ordDb.Remove(item.InsuredAsset);
                            }
                            if (item.Roles.Any())
                            {
                                // delete all roles
                                foreach (var role in item.Roles)
                                {
                                    if (role.Parties_PartyId.HasValue)
                                    {
                                        deleteParties.Add(new DeletePartyRequest(role.Parties_PartyId.Value));
                                    }
                                }
                                ordDb.RemoveRange(item.Roles);
                            }
                        }
                        saveOrder.Items.Clear();
                    }
                }
                else
                {
                    saveOrder = Order.CreateMtiOriginalSalesOrder(mtiSaleOrder.Number,
                        mtiSaleOrder.SaleDate,
                        mtiSaleOrder.LoanNumber);
                    ordDb.Add(saveOrder);
                    ordDb.SaveChanges();
                }

                // Create order items
                foreach (var respOrderItem in mtiSaleOrder.OrderItems)
                {
                    var mtiOriginalOrderItem = respOrderItem.Application;
                    var mtiOriginalProduct = respOrderItem.Product;

                    if (mtiOriginalOrderItem != null
                        && mtiOriginalProduct != null)
                    {
                        //assetDescs.Add($"{item.BRANDNAME} {item.MODELNAME} {item.NYRMANUF}");
                        //var orderItem = order.Items.OfType<MtiOriginalSalesOrderItem>()
                        //    .SingleOrDefault(x => x.OriginalId == item.ID);

                        var orderItem = saveOrder.CreateItem(
                                mtiOriginalOrderItem.Status,
                                mtiOriginalProduct.Name,
                                mtiOriginalProduct.PolicyType,
                                mtiOriginalProduct.Campaign,
                                mtiOriginalProduct.Package,
                                mtiOriginalProduct.Workshop);
                        saveOrder.AddItem(orderItem);
                        ordDb.SaveChanges();
                        respOrderItem.OrderItemId = orderItem.Id;

                        //ordDb.Find<Orders.Models.InsuredAsset>(orderItem.InsuredAssetId);
                        // stamp
                        //var stamp = Math.Ceiling(respOrderItem.NetPremium * 0.004m);
                        // vat
                        //var vat = Math.Round((respOrderItem.NetPremium + stamp) * 0.07m, 2);
                        orderItem.Price = respOrderItem.NetPremium;

                        orderItem.Quantity = 1;

                        orderItem.OriginalId = mtiOriginalOrderItem.OriginalId;
                        orderItem.TransID = mtiOriginalOrderItem.TransID;
                        orderItem.Remark = mtiOriginalOrderItem.Remark;
                        orderItem.RefNoticeNo = mtiOriginalOrderItem.RefNoticeNo;
                        orderItem.RefDetailNo = mtiOriginalOrderItem.RefDetailNo;
                        orderItem.StatusMessage = mtiOriginalOrderItem.StatusMessage;
                        orderItem.RefQuotation = mtiOriginalOrderItem.RefQuotation;
                        orderItem.Source = mtiOriginalOrderItem.Source;
                        orderItem.SystemId = mtiOriginalOrderItem.SystemId;
                        orderItem.CustomerInfoNo = mtiOriginalOrderItem.CustomerInfoNo;

                        orderItem.PayPlan = mtiOriginalOrderItem.PayPlan;
                        orderItem.CollateralNo = mtiOriginalOrderItem.CollateralNo;

                        orderItem.RefPolicyType = mtiOriginalProduct.RefPolicyType;
                        var coverage = respOrderItem.Coverage;
                        if (coverage != null)
                        {
                            orderItem.SumInsure = coverage.SumInsure;
                            orderItem.Deduct = coverage.Deduct;
                            orderItem.DamageLifePerPerson = coverage.DamageLifePerPerson;
                            orderItem.DamageLifePerTime = coverage.DamageLifePerTime;
                            orderItem.DamageInsurePerTime = coverage.DamageInsurePerTime;
                            orderItem.AccidentPerDriver = coverage.AccidentPerDriver;
                            orderItem.MedicalInsure = coverage.MedicalInsure;
                            orderItem.InsureDriver = coverage.InsureDriver;
                        }

                        // vehicle
                        var insuredAsset = respOrderItem.InsuredAsset;
                        if (insuredAsset != null
                            && insuredAsset.Vehicle != null)
                        {
                            var vehicle = insuredAsset.Vehicle;
                            orderItem.VehicleCode = vehicle.Code;
                            orderItem.VehicleBrand = vehicle.Brand;
                            orderItem.VehicleModel = vehicle.Model;
                            orderItem.VehicleManufactoringYear = vehicle.ManufactoringYear;
                            orderItem.VehicleChassis = vehicle.Chassis;
                            orderItem.VehicleColor = vehicle.Color;
                            orderItem.VehicleRegisterProvince = vehicle.RegisterProvince;
                            orderItem.VehicleRegisterYear = vehicle.ManufactoringYear;
                            orderItem.VehicleCc = vehicle.Cc;
                            orderItem.VehicleSeat = vehicle.Seat;
                            orderItem.VehicleWeight = vehicle.Weight;
                            orderItem.VehicleTonnage = vehicle.Tonnage;
                            orderItem.VehicleEngine = vehicle.Engine;
                            orderItem.VehiclePassenger = vehicle.Passenger;

                            var assetDescs = new List<string>();
                            if (!string.IsNullOrEmpty(vehicle.Brand)) assetDescs.Add(vehicle.Brand);
                            if (!string.IsNullOrEmpty(vehicle.Model)) assetDescs.Add(vehicle.Model);
                            if (vehicle.ManufactoringYear.HasValue) assetDescs.Add(vehicle.ManufactoringYear.Value.ToString("D"));

                            if (assetDescs.Any())
                            {
                                orderItem.InsuredAsset = new InsuredAsset(string.Join(",", assetDescs));
                                ordDb.SaveChanges();

                                insuredAsset.InsuredAssetId = orderItem.InsuredAsset.Id;
                            }
                        }

                        // set policiy
                        orderItem.PolicyPreviousNumber = mtiOriginalOrderItem.PolicyPreviousNumber;
                        orderItem.PolicyEffectiveDate = mtiOriginalOrderItem.PolicyEffectiveDate;
                        orderItem.PolicyExpiryDate = mtiOriginalOrderItem.PolicyExpiryDate;
                        if (respOrderItem.Policy != null)
                        {
                            var policy = respOrderItem.Policy;
                            orderItem.PolicyNumber = policy.Number;
                        }
                    }
                }

                mtiSaleOrder.OrderId = saveOrder.Id;

                saveOrder.TotalQuantity = saveOrder.Items.Sum(x => x.Quantity);

                ordDb.SaveChanges();

                var resp = getSavePartiesByOrderRequest(ordDb, mtiSaleOrder, orgTitleNames);
                resp.DeleteParties = deleteParties;
                resp.DeleteAssets = deleteAssets;

                await tran.CommitAsync(cancellationToken);

                return resp;
            }
        }

        private SavePartiesByOrderRequest getSavePartiesByOrderRequest(
            OrdersContext ordDb,
            MtiOriginalOrderResponse mtiSaleOrderResponse,
            IReadOnlyCollection<string> orgTitleNames)
        {
            var savePartiesByOrderRequest = new SavePartiesByOrderRequest(mtiSaleOrderResponse.OrderId.Value);

            var saleOrder = ordDb.Orders.OfType<MtiOriginalSalesOrder>()
                        .AsNoTracking()
                        .Include(x => x.Items)
                            .ThenInclude(y => y.Roles)
                                .ThenInclude(z => z.OrderRoleType)
                        .Single(x => x.Id == mtiSaleOrderResponse.OrderId);

            var savePartiesByOrderItemRequests = new List<SavePartiesByOrderItemRequest>();
            foreach (var item in mtiSaleOrderResponse.OrderItems)
            {
                var saleItem = saleOrder
                        .Items.OfType<MtiOriginalSalesOrderItem>()
                    .Single(x => x.Id == item.OrderItemId);

                var savePartiesByOrderItemRequest = new SavePartiesByOrderItemRequest(saleItem.Id.Value);
                savePartiesByOrderItemRequests.Add(savePartiesByOrderItemRequest);

                var parties = new List<PartyProfileRequest>();

                // set asset vehicles
                if (!string.IsNullOrEmpty(saleItem.VehicleChassis))
                {
                    var asset = new AssetRequest("VEHICLE", OrderRoleType.Owner, saleItem.VehicleChassis)
                    {
                        Vehicle = new VehicleAssetRequest()
                        {
                            Brand = saleItem.VehicleBrand,
                            Model = saleItem.VehicleModel,
                            Color = saleItem.VehicleColor,
                            RegisterNo = saleItem.VehicleRegisterNo,
                            RegisterProvince = saleItem.VehicleRegisterProvince,
                            RegisterYear = saleItem.VehicleRegisterYear,
                            Chassis = saleItem.VehicleChassis,
                            Cc = saleItem.VehicleCc,
                            Seat = saleItem.VehicleSeat,
                            Weight = saleItem.VehicleWeight,
                            Tonnage = saleItem.VehicleTonnage,
                            Engine = saleItem.VehicleEngine,
                            Passenger = saleItem.VehiclePassenger,
                        }
                    };

                    savePartiesByOrderItemRequest.InsuredAsset = asset;
                }


                if (item.Parties.Any())
                {
                    var insureds = item
                        .Parties.Where(x => x.RoleTypeCode == OrderRoleType.Insured)
                        .Where(x => x.IsOrganization.HasValue)
                        .ToList();
                    foreach (var insured in insureds)
                    {
                        // OWENER
                        // save insured party
                        var partyProfile = new PartyProfileRequest(OrderRoleType.Insured)
                        {
                            IsOrganization = insured.IsOrganization.Value,
                            Title = insured.TitleName,
                            FirstName = insured.FirstName,
                            LastName = insured.LastName,
                            MiddleName = insured.MiddleName,
                            BirthDate = insured.BirthDate,
                            CardId = insured.CardId,
                            Nationality = insured.Nationality,
                        };
                        parties.Add(partyProfile);

                        // set contact mechanisms
                        var postalAddresses = insured.PostalAddresses
                                .Where(x => x.ContactMechanismTypeCode == TransformMapExtensions.MainAddress)
                                .ToList();
                        var contactMechanismRequests = new List<ContactMechanismRequest>();
                        foreach (var postalAddress in postalAddresses)
                        {
                            var contact = new ContactMechanismRequest(TransformMapExtensions.MainAddress)
                            {
                                PostalAddress = new PostalAddressRequest()
                                {
                                    Name = postalAddress.Name,
                                    Village = postalAddress.Village,
                                    VillageNumber = postalAddress.VillageNumber,
                                    HouseNumber = postalAddress.HouseNumber,
                                    Floor = postalAddress.Floor,
                                    Room = postalAddress.Room,
                                    Building = postalAddress.Building,
                                    Alley = postalAddress.Alley,
                                    Road = postalAddress.Road,
                                    Province = postalAddress.Province,
                                    District = postalAddress.District,
                                    SubDistrict = postalAddress.SubDistrict,
                                    ZipCode = postalAddress.ZipCode,
                                }
                            };
                            contactMechanismRequests.Add(contact);
                        }

                        partyProfile.ContactMechanisms = contactMechanismRequests;

                    }

                    var invoices = item
                        .Parties.Where(x => x.RoleTypeCode == OrderRoleType.Invoice)
                        .Where(x => x.IsOrganization.HasValue)
                        .ToList();
                    foreach (var invoice in invoices)
                    {
                        // INVOICE
                        // save insured party
                        var partyProfile = new PartyProfileRequest(OrderRoleType.Invoice)
                        {
                            IsOrganization = invoice.IsOrganization.Value,
                            Title = invoice.TitleName,
                            FirstName = invoice.FirstName,
                            LastName = invoice.LastName,
                            MiddleName = invoice.MiddleName,
                            BirthDate = invoice.BirthDate,
                            CardId = invoice.CardId,
                            Nationality = invoice.Nationality,
                        };
                        parties.Add(partyProfile);
                    }
                }


                savePartiesByOrderItemRequest.Parties = parties;
            }

            savePartiesByOrderRequest.SaveRoleOrderItems = savePartiesByOrderItemRequests;

            return savePartiesByOrderRequest;
        }
    }
}
