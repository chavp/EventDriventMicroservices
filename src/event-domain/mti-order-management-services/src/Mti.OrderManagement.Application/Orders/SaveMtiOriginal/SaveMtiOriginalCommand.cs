using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Application.Abstractions.Messaging;
using Mti.Domain.Core.Primitives.Result;
using Mti.OrderManagement.Contracts.Orders;

namespace Mti.OrderManagement.Application.Orders.LoadMtiOriginal
{
    public sealed class SaveMtiOriginalCommand
        : ICommand<Result<MtiOriginalOrderResponse>>
    {
        public TranformMtiOriginalRequest? Request { get; set; }

        public SaveMtiOriginalCommand(TranformMtiOriginalRequest? request)
        {
            Request = request;
        }
    }
}
