using System.Threading.Channels;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace ProcessorMessageApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProcessorsController : ControllerBase
    {
        private readonly ILogger<ProcessorsController> _logger;
        public ProcessorsController(ILogger<ProcessorsController> logger)
        {
            _logger = logger;
        }
    }
}
