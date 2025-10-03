
using System.Net;
using System.Text;
using Ardalis.GuardClauses;
using Confluent.Kafka;
using FluentResults;
using FluentResults.Extensions;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Mti.PartyManagement.Domain.Parties;
using Mti.PartyManagement.Messaging;
using Mti.PartyManagement.Messaging.Parties;
using Mti.PartyManagement.Messaging.Parties.GetAssetsByIds;
using Mti.PartyManagement.Persistence;
using Mti.PartyManagement.Services.Api.Applications.GetAssetsByIds;
using Mti.PartyManagement.Services.Api.BackgroundServices.SavePartiesByOrder;
using Mti.PartyManagement.Services.Api.Infrastructure;
using Newtonsoft.Json;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Mti.PartyManagement.Services.Api.BackgroundServices.Parties
{
    public class PartyService : BackgroundService
    {
        private readonly ILogger<PartyService> _logger;
        protected readonly IDbContextFactory<PartiesContext> _dbFactory = null;
        private readonly string _bootstrapServers;

        private readonly GetAssetsByIdsHandler _getAssetsByIdsConsumer;

        public PartyService(
            IConfiguration configuration,
            ILogger<PartyService> logger,
            IDbContextFactory<PartiesContext> dbFactory,
            GetAssetsByIdsHandler getAssetsByIdsConsumer)
        {
            _logger = logger;
            _dbFactory = dbFactory;

            _bootstrapServers = Guard.Against.NullOrEmpty(configuration["Kafka:BootstrapServers"]);

            _getAssetsByIdsConsumer = getAssetsByIdsConsumer;

        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // GetAssetsByIds
            var task1 = Task.Run(() => _getAssetsByIdsConsumer.StartConsumingAsync(stoppingToken));

            // GetPartiesByIds
            var task2 = Task.Run(() => startGetPartiesByIds(stoppingToken));

            await Task.WhenAll(task1, task2);
        }

        private async Task startGetPartiesByIds(CancellationToken stoppingToken)
        {
            var topicName = "get_parties_by_ids";
            var topicRequest = $"dev.mti_party_management.{topicName}.request.th.v1";
            var topicResponse = $"dev.mti_party_management.{topicName}.response.th.v1";
            while (!stoppingToken.IsCancellationRequested)
            {
                
            }
        }
    }
}
