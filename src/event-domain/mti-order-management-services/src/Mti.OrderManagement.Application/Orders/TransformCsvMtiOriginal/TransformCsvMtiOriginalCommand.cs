using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Mti.Domain.Application.Abstractions.Messaging;
using Mti.Domain.Core.Primitives.Result;
using Mti.OrderManagement.Contracts.Orders;

namespace Mti.OrderManagement.Application.Orders.TransformCsvMtiOriginal
{
    public sealed class TransformCsvMtiOriginalCommand
        : ICommand<Result<TransformCsvMtiOriginalResponse>>
    {
        public IFormFile[] Request { get; set; }
        public int Page { get; set; } = 1;
        public int Limit { get; set; } = 100;

        public TransformCsvMtiOriginalCommand(IFormFile[] request)
        {
            Request = request;
        }
    }
}
