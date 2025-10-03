using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;
using Mti.Domain.Application.Abstractions.Messaging;
using Mti.Domain.Core.Primitives.Result;
using Mti.OrderManagement.Contracts.Orders;

namespace Mti.OrderManagement.Application.Orders.TransformMtiOriginal
{
    public sealed class TransformMtiOriginalCommand 
        : ICommand<Result<MtiOriginalOrderResponse>>
    {
        public TranformMtiOriginalRequest? Request { get; set; }
        public TransformMtiOriginalCommand(TranformMtiOriginalRequest? request)
        {
            Request = request;
        }
    }
}
