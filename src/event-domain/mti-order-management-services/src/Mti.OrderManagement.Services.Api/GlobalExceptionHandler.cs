using Microsoft.AspNetCore.Diagnostics;
using Mti.OrderManagement.Services.Api.Infrastructure;

namespace Mti.OrderManagement.Services.Api
{
    public class GlobalExceptionHandler : IExceptionHandler
    {
        private readonly ILogger<GlobalExceptionHandler> _logger;

        public GlobalExceptionHandler(ILogger<GlobalExceptionHandler> logger)
        {
            _logger = logger;
        }

        public async ValueTask<bool> TryHandleAsync(
            HttpContext httpContext,
            Exception exception,
            CancellationToken cancellationToken)
        {
            _logger.LogError(exception, "Exception occurred: {Message}", exception.Message);

            var problemDetails = httpContext.CreateProblemDetails(
                title: "Server Error",
                statusCode: StatusCodes.Status500InternalServerError
                );

            var isDevelopment = httpContext.RequestServices
                .GetService<IWebHostEnvironment>()?.IsDevelopment();
            if (isDevelopment.GetValueOrDefault())
            {
                problemDetails.Detail = exception.Message;
            }

            await httpContext.Response.WriteAsJsonAsync(problemDetails, cancellationToken);

            return true;
        }
    }
}
