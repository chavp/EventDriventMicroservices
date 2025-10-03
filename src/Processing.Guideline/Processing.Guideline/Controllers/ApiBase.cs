using FluentResults;
using Microsoft.AspNetCore.Mvc;

namespace Processing.Guideline.Controllers
{
    public abstract class ApiBase : ControllerBase
    {
        public static Error UnProcessableRequest => new Error(
                "General.UnProcessableRequest")
                .CausedBy(new NotImplementedException("The server could not process the request."));

        protected IActionResult BadRequest<IError>(IReadOnlyList<IError> errors)
        {
            var details = new List<string>();
            var codeErrors = new List<Domain.Errors.CodeError>();
            foreach (var error in errors)
            {
                if (error is Domain.Errors.CodeError cError)
                {
                    details.Add($"{cError.Code}:{cError.Message}");
                    codeErrors.Add(cError);
                }
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
                errors: codeErrors
                );

            return new ObjectResult(problemDetails);
        }

    }
}
