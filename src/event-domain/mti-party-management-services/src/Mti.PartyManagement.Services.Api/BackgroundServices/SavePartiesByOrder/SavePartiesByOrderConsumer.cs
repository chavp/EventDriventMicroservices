
using System.Collections.Immutable;
using System.Net;
using System.Text;
using Ardalis.GuardClauses;
using Confluent.Kafka;
using FluentResults;
using FluentResults.Extensions;
using Microsoft.EntityFrameworkCore;
using Mti.PartyManagement.Domain.Parties;
using Mti.PartyManagement.Domain.Parties.Types;
using Mti.PartyManagement.Messaging;
using Mti.PartyManagement.Persistence;
using Mti.PartyManagement.Services.Api.Infrastructure;
using Newtonsoft.Json;

namespace Mti.PartyManagement.Services.Api.BackgroundServices.SavePartiesByOrder
{
    public class SavePartiesByOrderConsumer : BackgroundService
    {
        //private readonly IConfiguration _configuration;
        private readonly ILogger<SavePartiesByOrderConsumer> _logger;
        private readonly string _requestTopic;
        private readonly string _responseTopic;
        private readonly string _bootstrapServers;
        private readonly string _groupId;

        protected readonly IDbContextFactory<PartiesContext> _dbFactory = null;

        private readonly IProducer<Guid, SavePartiesByOrderResponse> _producer;
        private readonly IConsumer<Guid, SavePartiesByOrderRequest> _consumer;

        public SavePartiesByOrderConsumer(
            IConfiguration configuration,
            ILogger<SavePartiesByOrderConsumer> logger,
            IDbContextFactory<PartiesContext> dbFactory)
        {
            Guard.Against.Null(configuration);
            _logger = Guard.Against.Null(logger);
            _dbFactory = Guard.Against.Null(dbFactory);

            _bootstrapServers = Guard.Against.NullOrEmpty(configuration["Kafka:BootstrapServers"]);

            _requestTopic = Guard.Against.NullOrEmpty(configuration["Kafka:SavePartiesByOrder:Consumer:RequestTopic"]);
            _responseTopic = Guard.Against.NullOrEmpty(configuration["Kafka:SavePartiesByOrder:Consumer:ResponseTopic"]);
            _groupId = Guard.Against.NullOrEmpty(configuration["Kafka:SavePartiesByOrder:Consumer:GroupId"]);

            _consumer = new ConsumerBuilder<Guid, SavePartiesByOrderRequest>(new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            })
            .SetValueDeserializer(new JsonDeserializer<SavePartiesByOrderRequest>())
            .SetKeyDeserializer(new JsonDeserializer<Guid>())
            .Build();

            _producer = new ProducerBuilder<Guid, SavePartiesByOrderResponse>(new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                ClientId = Dns.GetHostName(),
                EnableIdempotence = true,
                Acks = Acks.All,
            })
            .SetValueSerializer(new JsonSerializer<SavePartiesByOrderResponse>())
            .SetKeySerializer(new JsonSerializer<Guid>())
            .Build();
        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Run(() => startConsume(stoppingToken));
        }

        private async Task startConsume(CancellationToken stoppingToken)
        {
            _consumer.Subscribe(_requestTopic);

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<Guid, SavePartiesByOrderRequest> cr = null;
                try
                {
                    cr = _consumer.Consume(stoppingToken);

                    if (cr == null || cr.Message == null)
                    {
                        _consumer.Commit(cr);
                        continue;
                    }

                    _logger.LogDebug("Received message: {Key} - {Value}", 
                        cr.Message.Key, JsonConvert.SerializeObject(cr.Message.Value));

                    if (cr.Message.Headers.TryGetLastBytes("CorrelationId", out var corIdBytes))
                    {
                        var receivedCorrelationId = Encoding.UTF8.GetString(corIdBytes);

                        // Process the message
                        var request = cr.Message.Value;

                        // Send a response back if needed
                        _ = Task.Run(async () => await processSavePartiesByOrderRequestAsync(receivedCorrelationId,
                            request, stoppingToken));
                    }

                    _consumer.Commit(cr);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Error consuming messages from topic {Topic} {Reason}", _requestTopic, ex.Error.Reason);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "startConsume");
                    _consumer.Commit(cr);
                }
            }

            _consumer.Close();
        }

        private async Task<Result<SavePartiesByOrderResponse>> processSavePartiesByOrderRequestAsync(
            string correlationId,
            SavePartiesByOrderRequest request,
            CancellationToken stoppingToken)
        {
            // Send a response back if needed
            return await savePartiesByOrderRequestAsync(request, stoppingToken)
                    .Bind(resp => produceResponseAsync(
                        correlationId,
                        _responseTopic, resp, stoppingToken))
                    .MapErrors(errs =>
                    {
                        produceErrorAsync(
                            request.OrderId,
                            correlationId,
                            _responseTopic, errs, stoppingToken)
                            .Wait();
                        return errs;
                    });
        }

        private async Task<Result<SavePartiesByOrderResponse>> savePartiesByOrderRequestAsync(SavePartiesByOrderRequest request, CancellationToken cancellationToken)
        {
            var response = new SavePartiesByOrderResponse(request.OrderId)
            {
                Orders_TenantId = request.Orders_TenantId,
            };

            try
            {

                var savePartiesByOrderItemResponses = new List<SavePartiesByOrderItemResponse>();
                using (var partiesDb = await _dbFactory.CreateDbContextAsync(cancellationToken))
                using (var tran = await partiesDb.Database.BeginTransactionAsync(cancellationToken))
                {
                    // Clean all parties, contacts and assets
                    foreach (var deleteParty in request.DeleteParties)
                    {
                        var party = partiesDb.Parties
                            .Include(x => x.AssetRoles)
                                .ThenInclude(y => y.Asset)
                            .Include(x => x.ContactMechanisms)
                                .ThenInclude(y => y.ContactMechanismType)
                            .SingleOrDefault(x => x.Id == deleteParty.PartyId);
                        if (party != null)
                        {
                            // remove asset roles
                            if (party.AssetRoles.Any())
                            {
                                partiesDb.RemoveRange(party.AssetRoles);
                            }

                            // remove contact mechanisms
                            if (party.ContactMechanisms.Any())
                            {
                                partiesDb.RemoveRange(party.ContactMechanisms);
                            }

                            // remove party
                            partiesDb.Remove(party);

                            partiesDb.SaveChanges();
                        }
                    }

                    foreach (var deleteAsset in request.DeleteAssets)
                    {
                        var asset = partiesDb.Assets
                            .SingleOrDefault(x => x.Id == deleteAsset.AssetId);
                        if (asset != null)
                        {
                            // remove asset
                            partiesDb.Remove(asset);
                            partiesDb.SaveChanges();
                        }
                    }

                    foreach (var saveRoleOrderItemRequest in request.SaveRoleOrderItems)
                    {
                        var savePartiesByOrderItemResponse = new SavePartiesByOrderItemResponse(saveRoleOrderItemRequest.OrderItemId);
                        savePartiesByOrderItemResponses.Add(savePartiesByOrderItemResponse);

                        // save party
                        var partyProfileResponses = new List<PartyProfileResponse>();
                        foreach (var party in saveRoleOrderItemRequest.Parties)
                        {
                            // save insured party
                            var partyProfileResponse = addSingleParty(partiesDb, party);
                            partyProfileResponses.Add(partyProfileResponse);
                        }

                        if (saveRoleOrderItemRequest.InsuredAsset != null)
                        {
                            var insuredPartyIds = partyProfileResponses
                                .Where(x => x.PartyRoleTypeCode == PartyRoleType.Insured)
                                .Select(x => x.PartyId)
                                .ToList();

                            // save asset
                            var assetResponse = addAsset(partiesDb, insuredPartyIds, saveRoleOrderItemRequest.InsuredAsset);
                            if (assetResponse != null)
                            {
                                savePartiesByOrderItemResponse.Asset = assetResponse;
                            }
                        }

                        savePartiesByOrderItemResponse.Parties = partyProfileResponses;
                    }
                    
                    await tran.CommitAsync(cancellationToken);
                }

                response.SaveRoleOrderItems = savePartiesByOrderItemResponses;
            }
            catch(Exception ex)
            {
                return Result.Fail(new FluentResults.Error("Error from savePartiesByOrderRequestAsync")
                    .CausedBy(ex));
            }

            return Result.Ok(response);
        }
        private async Task<Result> produceResponseAsync(
            string correlationId, 
            string replyToTopic,
            SavePartiesByOrderResponse response, 
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Producing response for: {resp}", JsonConvert.SerializeObject(response));
            try
            {
                //throw new NotImplementedException("Test Error");
                var message = new Message<Guid, SavePartiesByOrderResponse>
                {
                    Key = response.OrderId,
                    Value = response,
                    Headers = new Headers
                    {
                        { "CorrelationId", Encoding.UTF8.GetBytes(correlationId) }
                    }
                };
                var deliveryResult = await _producer
                        .ProduceAsync(replyToTopic, message, cancellationToken);
                _logger.LogInformation($"Response order id = {response.OrderId}, reply = {replyToTopic}, correlationId = {correlationId}");

                return Result.Ok();
            }
            catch (ProduceException<Guid, SavePartiesByOrderResponse> ex)
            {
                _logger.LogError(ex, "Failed to send message");
                return Result.Fail(new FluentResults.Error("Failed to send message")
                    .CausedBy(ex));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An unexpected error occurred while sending message");
                return Result.Fail(new FluentResults.Error("An unexpected error occurred while sending message")
                    .CausedBy(ex));
            }

        }

        private async Task<Result> produceErrorAsync(
            Guid orderId,
            string correlationId,
            string replyToTopic,
            IError error,
            CancellationToken cancellationToken)
        {
            try
            {
                var errorTitle = error.Message;
                var errorMessage = string.Join(",", error.Reasons.Select(x => x.Message));
                var message = new Message<Guid, SavePartiesByOrderResponse>
                {
                    Key = orderId,
                    Value = new SavePartiesByOrderResponse(Guid.NewGuid()),
                    Headers = new Headers
                    {
                        { "CorrelationId", Encoding.UTF8.GetBytes(correlationId) },
                        { "Title", Encoding.UTF8.GetBytes(errorTitle) },
                        { "Error", Encoding.UTF8.GetBytes(errorMessage) },
                    }
                };
                var deliveryResult = await _producer
                        .ProduceAsync(replyToTopic, message, cancellationToken);
                _logger.LogInformation($"Response error correlationId = {correlationId}");
            }
            catch (ProduceException<Guid, SavePartiesByOrderResponse> ex)
            {
                _logger.LogError(ex, "Failed to send message");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An unexpected error occurred while sending message");
            }
            return Result.Ok();
        }


        //private async Task produceResponseAsync(
        //    string correlationId, 
        //    string replyToTopic,
        //    string error, 
        //    CancellationToken cancellationToken)
        //{
        //    try
        //    {
        //        var message = new Message<Guid, SavePartiesByOrderResponse>
        //        {
        //            Key = response.OrderId,
        //            Value = response,
        //            Headers = new Headers
        //            {
        //                { "CorrelationId", Encoding.UTF8.GetBytes(correlationId) },
        //                { "ReplyToTopic", Encoding.UTF8.GetBytes(replyToTopic) }
        //            }
        //        };
        //        var deliveryResult = await _producer
        //                .ProduceAsync(replyToTopic, message, cancellationToken);
        //        _logger.LogInformation($"Response order id = {response.OrderId}");
        //    }
        //    catch (ProduceException<Guid, SavePartiesByOrderResponse> ex)
        //    {
        //        _logger.LogError(ex, "Failed to send message");
        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError(ex, "An unexpected error occurred while sending message");
        //    }
        //}

        private PartyProfileResponse addSingleParty(PartiesContext partiesDb
            , PartyProfileRequest partyProfile)
        {

            var nationality = getNationality(partiesDb, partyProfile.Nationality);
            var partyTitle = saveTitle(partiesDb, partyProfile.Title, partyProfile.IsOrganization);

            Party? party = null;
            if (partyProfile.IsOrganization)
            {
                party = new LegalOrganization(partyProfile.CardId)
                {
                    PartyTitle = partyTitle,
                    Name = partyProfile.FirstName,
                    Reference = partyProfile.LastName,
                    Nationality = nationality,
                };
            }
            else
            {
                party = new Person
                {
                    PartyTitle = partyTitle,
                    FirstName = partyProfile.FirstName,
                    MiddleName = partyProfile.MiddleName,
                    LastName = partyProfile.LastName,
                    CardId = partyProfile.CardId,
                    BirthDate = partyProfile.BirthDate,
                    Nationality = nationality,
                };
            }

            partiesDb.Add(party);
            partiesDb.SaveChanges();

            var partyRoleType = partiesDb.PartyRoleTypes
                .Single(x => x.Code == partyProfile.PartyRoleTypeCode);
            if (partyRoleType.Code == PartyRoleType.Insured)
            {
                var insured = new Insured
                {
                    Party = party,
                    PartyRoleType = partyRoleType,
                };
                partiesDb.Add(insured);
                partiesDb.SaveChanges();
            }
            else if (partyRoleType.Code == PartyRoleType.Invoice)
            {
                var invoice = new Invoice
                {
                    Party = party,
                    PartyRoleType = partyRoleType,
                };
                partiesDb.Add(invoice);
                partiesDb.SaveChanges();
            }

            var response = new PartyProfileResponse(party.Id.Value, partyRoleType.Code);

            // save contact
            var contactRespList = new List<ContactMechanismResponse>();
            foreach (var contact in partyProfile.ContactMechanisms)
            {
                var contactType = partiesDb
                        .ContactMechanismTypes
                        .Single(x => x.Code == contact.ContactMechanismTypeCode);

                if (contact.PostalAddress is PostalAddressRequest postalAddressRequest)
                {
                    var contMech = new PostalAddresse
                    {
                        ContactMechanismType = contactType,
                        Name = postalAddressRequest.Name,
                    };
                    contMech.Village = postalAddressRequest.Village;
                    contMech.VillageNumber = postalAddressRequest.VillageNumber;
                    contMech.HouseNumber = postalAddressRequest.HouseNumber;
                    contMech.Floor = postalAddressRequest.Floor;
                    contMech.Room = postalAddressRequest.Room;
                    contMech.Building = postalAddressRequest.Building;
                    contMech.Alley = postalAddressRequest.Alley;
                    contMech.Road = postalAddressRequest.Road;
                    contMech.Province = postalAddressRequest.Province;
                    contMech.District = postalAddressRequest.District;
                    contMech.SubDistrict = postalAddressRequest.SubDistrict;
                    contMech.ZipCode = postalAddressRequest.ZipCode;

                    partiesDb.Add(contMech);
                    partiesDb.SaveChanges();

                    party.ContactMechanisms.Add(contMech);
                    partiesDb.SaveChanges();

                    var contactMechanismResponse = new ContactMechanismResponse(contMech.Id.Value, contactType.Code);
                    contactRespList.Add(contactMechanismResponse);
                }
            }
            response.ContactMechanisms = contactRespList;

            return response;
        }

        private Nationality? getNationality(
            PartiesContext db,
            string cnationlity)
        {
            var nation = cnationlity.CleanNull();
            if (string.IsNullOrEmpty(nation)) return null;

            var code = nation.GenCode();
            var target = db.Nationalities
                .SingleOrDefault(x => x.Code == code);
            if (target == null)
            {
                target = new Nationality(code)
                {
                    Name = nation
                };
                db.Add(target);
                db.SaveChanges();
            }
            return target;
        }

        private PartyTitle? saveTitle(
            PartiesContext db,
            string titleName,
            bool isOrganization)
        {
            if (string.IsNullOrEmpty(titleName)) return null;
            if (!isOrganization) return null;
            var titleCode = titleName.GenCode();

            var orgTitles = db.PartyTitles
                .Where(x => x.IsOrganization == isOrganization)
                .ToImmutableList();
            var partyTitle = orgTitles
                        .SingleOrDefault(x => titleName
                        .Equals(x.Name, StringComparison.InvariantCultureIgnoreCase));
            if (partyTitle == null)
            {
                partyTitle = new PartyTitle(titleCode)
                {
                    Name = titleName,
                    IsOrganization = isOrganization
                };
                db.Add(partyTitle);
                db.SaveChanges();
            }

            return partyTitle;
        }

        private AssetResponse? addAsset(PartiesContext partiesDb
            , List<Guid> ownerPartyIds
            , AssetRequest assetRequest)
        {
            // save asset
            var assetOwnerType = partiesDb
                        .AssetRoleTypes
                        .SingleOrDefault(x => x.Code == assetRequest.AssetRoleTypeCode);

            if (assetRequest.Vehicle is VehicleAssetRequest vehReq)
            {
                var veh = new Vehicle(vehReq.Chassis);
                veh.Code = vehReq.Code;
                veh.Brand = vehReq.Brand;
                veh.Model = vehReq.Model;
                veh.Color = vehReq.Color;
                veh.RegisterNo = vehReq.RegisterNo;
                veh.RegisterProvince = vehReq.RegisterProvince;
                veh.RegisterYear = vehReq.RegisterYear;
                veh.Chassis = vehReq.Chassis;
                veh.Cc = vehReq.Cc;
                veh.Seat = vehReq.Seat;
                veh.Weight = vehReq.Weight;
                veh.Tonnage = vehReq.Tonnage;
                veh.Engine = vehReq.Engine;
                veh.Passenger = vehReq.Passenger;
                partiesDb.Add(veh);
                partiesDb.SaveChanges();

                foreach (var ownerPartyId in ownerPartyIds)
                {
                    var assetRole = new AssetRole(ownerPartyId, assetOwnerType.Id, veh.Id);
                    partiesDb.Add(assetRole);
                    partiesDb.SaveChanges();
                }

                return new AssetResponse(veh.Id.Value, assetOwnerType.Code);
            }

            return null;
        }
    }
}
