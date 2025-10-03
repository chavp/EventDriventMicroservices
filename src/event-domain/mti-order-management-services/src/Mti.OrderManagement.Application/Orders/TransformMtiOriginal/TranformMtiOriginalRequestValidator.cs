using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentValidation;
using Mti.Domain.Application.Core.Extensions;
using Mti.OrderManagement.Contracts.Orders;
using Mti.OrderManagement.Domain.Errors;

namespace Mti.OrderManagement.Application.Orders.TransformMtiOriginal
{
    public sealed class TranformMtiOriginalRequestValidator : AbstractValidator<TranformMtiOriginalRequest>
    {
        public TranformMtiOriginalRequestValidator()
        {
            RuleFor(x => x.LoanNumber).NotEmpty().WithError(ValidationErrors.TransformMtiOriginal.LoanNumberIsRequired);
            RuleFor(x => x.SaleDate).NotEmpty().WithError(ValidationErrors.TransformMtiOriginal.SaleDateIsRequired);
            RuleFor(x => x.Items.Any())
                .NotEqual(false)
                .WithError(ValidationErrors.TransformMtiOriginal.ItemsIsRequired);
        }
    }
}
