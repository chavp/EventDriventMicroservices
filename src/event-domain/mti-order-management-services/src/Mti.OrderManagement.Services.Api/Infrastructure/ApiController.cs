using System.Collections.ObjectModel;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Mti.Domain.Core.Primitives;
using Mti.OrderManagement.Services.Api.Contracts;

namespace Mti.OrderManagement.Services.Api.Infrastructure
{
    //[Authorize]
    [ApiController]
    [Route("api/v{v:apiVersion}")]
    public class ApiController : ControllerBase
    {
        protected IActionResult BadRequest(IReadOnlyCollection<Error> errors)
        {
            var details = new List<string>();
            foreach (var error in errors)
            {
                details.Add($"{error.Code}:{error.Message}");
            }

            var prob1 = base.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                title: "Bad Request",
                type: "https://datatracker.ietf.org/doc/html/rfc7231#sectio-6.6.1",
                detail: string.Join(", ", details)
                );

            var problemDetails = HttpContext.CreateProblemDetails(
                title: "Bad Request",
                statusCode: StatusCodes.Status400BadRequest,
                errors: errors
                );

            return new ObjectResult(problemDetails);
        }

        protected new IActionResult Ok(object value) => base.Ok(value);

        protected new IActionResult NotFound() => base.NotFound();
    }
}
