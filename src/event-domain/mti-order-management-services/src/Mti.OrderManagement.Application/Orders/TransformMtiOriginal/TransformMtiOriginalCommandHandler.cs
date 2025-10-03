using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Core;
using FluentValidation;
using Mti.Domain.Application.Abstractions.Common;
using Mti.Domain.Application.Abstractions.Data;
using Mti.Domain.Application.Abstractions.Messaging;
using Mti.Domain.Core.Errors;
using Mti.Domain.Core.Primitives.Maybe;
using Mti.Domain.Core.Primitives.Result;
using Mti.OrderManagement.Contracts.Extensions;
using Mti.OrderManagement.Contracts.Orders;
using Mti.OrderManagement.Domain.Orders.Types;
using Mti.OrderManagement.Persistence.Repositories;

namespace Mti.OrderManagement.Application.Orders.TransformMtiOriginal
{
    public sealed class TransformMtiOriginalCommandHandler 
        : ICommandHandler<TransformMtiOriginalCommand, Result<MtiOriginalOrderResponse>>
    {
        private readonly IPartyRepository _partiesRepository;
        public TransformMtiOriginalCommandHandler(IPartyRepository partiesRepository)
        {
            _partiesRepository = partiesRepository;
        }

        public async Task<Result<MtiOriginalOrderResponse>> Handle(TransformMtiOriginalCommand command, CancellationToken cancellationToken)
        {
            IReadOnlyCollection<string> orgTitleNames = _partiesRepository.GetOrganizationTitles();
            return Result
                .Create(command.Request, [DomainErrors.General.UnProcessableRequest])
                .Map(orgTitleNames);
        }
    }


}
