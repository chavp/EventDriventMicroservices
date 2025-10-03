using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentResults;
using Mti.ProductManagement.Messaging.Products.Commands;

namespace Mti.ProductManagement.Application.Products.SaveProductMtiOriginal
{
    public sealed class SaveProductMtiOriginalCommand 
        : ICommand<Result<SaveProductsByOrderResponse>>
    {
        public SaveProductsByOrderRequest? Request { get; }
        public SaveProductMtiOriginalCommand(SaveProductsByOrderRequest? request)
        {
            Request = request ?? throw new ArgumentNullException(nameof(request), "Request cannot be null");
        }

    }

    public interface ICommand<out TResponse>
    {
    }
}
