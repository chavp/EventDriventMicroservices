using System.Net;
using Ardalis.GuardClauses;
using Confluent.Kafka;
using FluentResults;
using Microsoft.EntityFrameworkCore;
using Mti.PartyManagement.Messaging.Parties.GetAssetsByIds;
using Mti.PartyManagement.Persistence;
using Mti.PartyManagement.Services.Api.BackgroundServices.Parties;
using Mti.PartyManagement.Services.Api.Infrastructure;

namespace Mti.PartyManagement.Services.Api.Applications
{
    public interface IQueryApplicationHandler<TQuery, TResponse>
    {
        public Task StartConsumingAsync(CancellationToken stoppingToken);
        public Task<Result<TResponse>> HandleAsync(
            TQuery query,
            CancellationToken stoppingToken);
    }
}
