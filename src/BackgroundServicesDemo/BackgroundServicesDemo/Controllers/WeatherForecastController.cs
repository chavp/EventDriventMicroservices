using BackgroundServicesDemo.Contracts;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Channels;

namespace BackgroundServicesDemo.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private readonly Channel<FileImportRequest> _channel;

        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;

        public WeatherForecastController(ILogger<WeatherForecastController> logger,
            Channel<FileImportRequest> channel)
        {
            _logger = logger;
            _channel = channel;
        }

        [HttpGet(Name = "GetWeatherForecast")]
        public IEnumerable<WeatherForecast> Get()
        {
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
        }

        [HttpPost(Name = "UploadFile")]
        public async Task<IEnumerable<WeatherForecast>> UploadFile(IFormFile[] files)
        {
            foreach (var file in files)
            {
                using var mem = new MemoryStream();
                file.CopyTo(mem);

                var fileRequest = new FileImportRequest
                {
                    FileName = file.FileName,
                    FileContent = mem.ToArray(),
                };

                await _channel.Writer.WriteAsync(fileRequest);
            }

            return Enumerable.Empty<WeatherForecast>();
        }
    }
}
