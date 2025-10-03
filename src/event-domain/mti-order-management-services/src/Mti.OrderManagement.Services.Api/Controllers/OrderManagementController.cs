using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Text;
using System.Threading.Channels;
using Asp.Versioning;
using Confluent.Kafka;
using FluentValidation;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Mti.Domain.Core.Primitives.Maybe;
using Mti.Domain.Core.Primitives.Result;
using Mti.Domain.Infrastructure.Common;
using Mti.OrderManagement.Application.Orders.LoadMtiOriginal;
using Mti.OrderManagement.Application.Orders.SavePartiesByOrder;
using Mti.OrderManagement.Application.Orders.TransformCsvMtiOriginal;
using Mti.OrderManagement.Application.Orders.TransformMtiOriginal;
using Mti.OrderManagement.Contracts.Extensions;
using Mti.OrderManagement.Contracts.Orders;
using Mti.OrderManagement.Domain.Orders;
using Mti.OrderManagement.Messaging;
using Mti.OrderManagement.Persistence;
using Mti.OrderManagement.Persistence.Repositories;
using Mti.OrderManagement.Services.Api.Contracts;
using Mti.OrderManagement.Services.Api.Infrastructure;
using Mti.PartyManagement.Messaging;
using Mti.PartyManagement.Messaging.Parties.GetAssetsByIds;
using Mti.ProductManagement.Messaging.Products.Commands;
using static System.Runtime.InteropServices.JavaScript.JSType;
using static Confluent.Kafka.ConfigPropertyNames;
using Mti.Domain.Infrastructure.Extensions;

namespace Mti.OrderManagement.Services.Api.Controllers
{
    [ApiVersion(1)]
    public sealed class OrderManagementController : ApiController
    {
        private readonly IPartyRepository _partiesRepository;
        private readonly IDbContextFactory<OrdersContext> _dbContextFactory;

        private readonly SavePartiesByOrderProducer _savePartiesByOrderProducer;

        private readonly ILogger _logger;

        private readonly Channel<SaveProductsByOrderRequest> _saveProductsByOrderRequestChannel;
        private readonly Channel<MtiOriginalOrderResponse> _mtiOriginalOrderResponseChannel;

        public OrderManagementController(
            ILogger<OrderManagementController> logger,
            IPartyRepository partiesRepository,
            IDbContextFactory<OrdersContext> dbContextFactory,
            SavePartiesByOrderProducer savePartiesByOrderProducer,
            Channel<SaveProductsByOrderRequest> saveProductByOrderRequestChannel,
            Channel<MtiOriginalOrderResponse> mtiOriginalOrderResponseChannel)
        {
            _logger = logger;
            _partiesRepository = partiesRepository;
            _dbContextFactory = dbContextFactory;
            _savePartiesByOrderProducer = savePartiesByOrderProducer;
            _saveProductsByOrderRequestChannel = saveProductByOrderRequestChannel;
            _mtiOriginalOrderResponseChannel = mtiOriginalOrderResponseChannel;
        }

        [HttpPost(ApiRoutes.OrderManagement.TransformMtiOriginal)]
        [ProducesResponseType(typeof(MtiOriginalOrderResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(typeof(ApiErrorResponse), StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> TransformMtiOriginal(IValidator<TranformMtiOriginalRequest> validator, TranformMtiOriginalRequest extractMtiOriginalRequest, CancellationToken cancellationToken) =>
            await Result.Create(extractMtiOriginalRequest, [Mti.Domain.Core.Errors.DomainErrors.General.UnProcessableRequest])
                .PreValidate(validator)    
                .Map(request => new TransformMtiOriginalCommand(extractMtiOriginalRequest))
                .Bind(command => new TransformMtiOriginalCommandHandler(_partiesRepository).Handle(command, cancellationToken))
                .Match(Ok, BadRequest);

        [HttpPost(ApiRoutes.OrderManagement.TransformCsvMtiOriginal)]
        [ProducesResponseType(typeof(TransformCsvMtiOriginalResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(typeof(ApiErrorResponse), StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> TransformCsvMtiOriginal(IFormFile[] files,
                CancellationToken cancellationToken,
                [FromQuery] int page = 1, [FromQuery] int limit = 100) =>
            await Result.Create(files, [Mti.Domain.Core.Errors.DomainErrors.General.UnProcessableRequest])
                .Map(request => new TransformCsvMtiOriginalCommand(request) { Page = page, Limit = limit })
                .Bind(command => new TransformCsvMtiOriginalCommandHandler().Handle(command, cancellationToken))
                .Match(Ok, BadRequest);

        [HttpPut(ApiRoutes.OrderManagement.SaveMtiOriginal)]
        [ProducesResponseType(typeof(IReadOnlyCollection<TranformMtiOriginalRequest>), StatusCodes.Status200OK)]
        [ProducesResponseType(typeof(ApiErrorResponse), StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> SaveMtiOriginal(
            IValidator<TranformMtiOriginalRequest> validator,
            TranformMtiOriginalRequest request, CancellationToken cancellationToken) =>
           await Result.Create(request, [Mti.Domain.Core.Errors.DomainErrors.General.UnProcessableRequest])
                .PreValidate(validator)
                .Map(request => new SaveMtiOriginalCommand(request))
                .Bind(command => new SaveMtiOriginalCommandHandler(
                   _logger,
                   _dbContextFactory, 
                   _partiesRepository,
                   _saveProductsByOrderRequestChannel,
                   _mtiOriginalOrderResponseChannel,
                   _savePartiesByOrderProducer)
               .Handle(command, cancellationToken))
               .Match(Ok, BadRequest);

        [HttpGet(ApiRoutes.OrderManagement.GetMtiOriginalOrderById)]
        [ProducesResponseType(typeof(MtiOriginalOrderResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> GetMtiOriginalOrderById(
            Guid orderId, CancellationToken cancellationToken)
        {
            using (var db = _dbContextFactory.CreateDbContext())
            {
                var order = db.Orders.OfType<MtiOriginalSalesOrder>()
                    .Include(o => o.Items)
                        .ThenInclude(oItem => oItem.Roles)
                            .ThenInclude(oItemRole => oItemRole.OrderRoleType)
                    .SingleOrDefault(x => x.Id == orderId);

                if(order == null) return NotFound();

                var orderTypeCode = "MTI_ORIGINAL";
                var respBuilder = MtiOriginalOrderResponse
                    .CreateBuilder(orderTypeCode, order.SaleDate.Value, order.LoanNumber)
                    .WithOrders_TenantId(null)
                    .WithProducts_TenantId(order.Products_TenantId)
                    .WithParties_TenantId(order.Parties_TenantId)
                ;
                foreach (var item in order.Items)
                {
                    if(item is MtiOriginalSalesOrderItem mtiItem)
                    {
                        var orderItem = MtiOriginalOrderItemResponse
                            .CreateBuilder(orderTypeCode)
                            .WithNetPremium(mtiItem.Price)
                            .WithSeq(mtiItem.Seq)
                            .WithApplication(
                                orderTypeCode, app => app
                                    .WithOriginalId(mtiItem.OriginalId)
                                    .WithTransID(mtiItem.TransID)
                                    .WithStatus(mtiItem.Status)
                                    .WithRemark(mtiItem.Remark)
                                    .WithSource(mtiItem.Source)
                                    .WithSystemId(mtiItem.SystemId)
                                    .WithRefNoticeNo(mtiItem.RefNoticeNo)
                                    .WithRefDetailNo(mtiItem.RefDetailNo)
                                    .WithStatusMessage(mtiItem.StatusMessage)
                                    .WithRefQuotation(mtiItem.RefQuotation)
                                    .WithPayPlan(mtiItem.PayPlan)
                                    .WithCollateralNo(mtiItem.CollateralNo)
                                    .WithCustomerInfoNo(mtiItem.CustomerInfoNo)
                                    .WithPolicyType(mtiItem.PolicyType)
                                    .WithPolicyNumber(mtiItem.PolicyNumber)
                                    .WithPolicyPreviousNumber(mtiItem.PolicyPreviousNumber)
                                    .WithPolicyEffectiveDate(mtiItem.PolicyExpiryDate)
                                    .WithPolicyExpiryDate(mtiItem.PolicyExpiryDate)
                                    .ValidatePolicyDates()
                            )
                            .WithProduct(orderTypeCode,
                                mtiItem.ProductName,
                                mtiItem.PolicyType,
                                p => p
                                .WithCampaign(mtiItem.Campaign)
                                .WithPackage(mtiItem.Package)
                                .WithWorkshop(mtiItem.Workshop)
                                .WithRefPolicyType(mtiItem.RefPolicyType)
                            )
                            .WithCoverage(orderTypeCode, cov => cov
                                .WithSumInsure(mtiItem.SumInsure)
                                .WithDeduct(mtiItem.Deduct)
                                .WithDamageLifePerPerson(mtiItem.DamageLifePerPerson)
                                .WithDamageLifePerTime(mtiItem.DamageLifePerTime)
                                .WithDamageInsurePerTime(mtiItem.DamageInsurePerTime)
                                .WithAccidentPerDriver(mtiItem.AccidentPerDriver)
                                .WithMedicalInsure(mtiItem.MedicalInsure)
                                .WithInsureDriver(mtiItem.InsureDriver)
                            )
                            .WithPolicy(poli => poli
                                .WithNumber(mtiItem.PolicyNumber)
                            )
                            .Build();

                        respBuilder.AddOrderItem(orderItem);

                        if(mtiItem.InsuredAssetId.HasValue)
                        {
                            db.Find<InsuredAsset>(mtiItem.InsuredAssetId);

                            // parties get asset
                            if (mtiItem.InsuredAsset.Parties_AssetId.HasValue)
                            {
                                var topicName = "get_assets_by_ids";
                                var topicRequest = $"dev.mti_party_management.{topicName}.request.th.v1";
                                var topicResponse = $"dev.mti_party_management.{topicName}.response.th.v1";
                                var bootstrapServers = "localhost:9092";
                                var config = new AdminClientConfig { BootstrapServers = bootstrapServers };
                                var adminClient = new AdminClientBuilder(config).Build();
                                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

                                var autoDeleteReplyTopicService = new AutoDeleteReplyTopicService(_logger
                                    , adminClient, "dev");
                                var replyTopic = await autoDeleteReplyTopicService
                                    .CreateTemporaryReplyTopic(Guid.NewGuid().ToString());

                                var assetResp = await getAssetsByIdsAsync(
                                    bootstrapServers,
                                    topicRequest,
                                    replyTopic,
                                    new GetAssetsByIdsQuery
                                    {
                                        AssetIds = [mtiItem.InsuredAsset.Parties_AssetId.Value]
                                    }, cancellationToken);
                                if (assetResp != null)
                                {
                                    var assData = assetResp
                                        .Data
                                        .SingleOrDefault(x => x.AssetId == mtiItem.InsuredAsset.Parties_AssetId.Value
                                        && x.Vehicle != null);
                                    if (assData != null)
                                    {
                                        orderItem.InsuredAsset = AssetValue
                                                .CreateBuilder(assData.AssetTypeCode)
                                                .WithAssetId(assData.AssetId)
                                                .WithInsuredAssetId(mtiItem.InsuredAsset.Id)
                                                .WithDescription(assData.Vehicle.Chassis)
                                                .WithVehicle(veh => veh
                                                    .WithCode(assData.Vehicle.Code)
                                                    .WithBrand(assData.Vehicle.Brand)
                                                    .WithModel(assData.Vehicle.Model)
                                                    .WithManufactoringYear(assData.Vehicle.ManufactoringYear)
                                                    .WithChassis(assData.Vehicle.Chassis)
                                                    .WithColor(assData.Vehicle.Color)
                                                    .WithRegisterProvince(assData.Vehicle.RegisterProvince)
                                                    .WithRegisterYear(assData.Vehicle.RegisterYear)
                                                    .WithCc(assData.Vehicle.Cc)
                                                    .WithSeat(assData.Vehicle.Seat)
                                                    .WithWeight(assData.Vehicle.Weight)
                                                    .WithTonnage(assData.Vehicle.Tonnage)
                                                    .WithEngine(assData.Vehicle.Engine)
                                                    .WithPassenger(assData.Vehicle.Passenger)
                                                )
                                                .Build();
                                    }
                                }
                            }
                        }


                    }
                    else if (item is SalesOrderItem sailItem)
                    {

                    }

                    // get parties
                    if (item.Roles.Any())
                    {
                        var partyIds = item.Roles
                            .Select(role => role.Parties_PartyId)
                            .ToList();

                        // get praties by ids
                    }
                }

                return Ok(respBuilder.Build());
            }
        }

        private async Task<GetAssetsByIdsResponse> getAssetsByIdsAsync(
            string bootstrapServers,
            string topicRequest,
            string topicResponse,
            GetAssetsByIdsQuery request, 
            CancellationToken stoppingToken)
        {
            return await request.GetRequestResponseAsync<GetAssetsByIdsQuery,GetAssetsByIdsResponse>(
                bootstrapServers,
                topicRequest,
                topicResponse,
                _logger,
                stoppingToken);
        }
    }
}
