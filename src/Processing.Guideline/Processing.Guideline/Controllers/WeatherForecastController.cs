using FluentResults;
using Microsoft.AspNetCore.Mvc;
using Processing.Guideline.Contracts;
using Error = FluentResults.Error;
using Processing.Guideline;

using static Processing.Guideline.Domain.Errors;
using Processing.Guideline.Applications;

namespace Processing.Guideline.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ApiBase
    {
        ValidationService _validationService;
        public WeatherForecastController() 
        {
            _validationService = new ValidationService();
        }

        [HttpPut(Name = "validations")]
        public async Task<IActionResult> ValidationProcess(DataRequest request)
            => await ResultExtensions
                .Create(request, [UnProcessableRequest])
                .Bind(_validationService.ValidationProcess)
                .Match(Ok, BadRequest);
    }
    
}
