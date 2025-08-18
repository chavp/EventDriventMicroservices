using System.Threading.Channels;
using Microsoft.AspNetCore.Mvc;
using Send.Messaging.Send;
using SendMessageApi.BackgroundTasks;

namespace SendMessageApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SendersController : ControllerBase
    {
        private readonly ILogger<SendersController> _logger;
        private readonly Channel<SendRequest> _channel;
        public SendersController(ILogger<SendersController> logger, 
            Channel<SendRequest> channel)
        {
            _logger = logger;
            _channel = channel;
        }

        [HttpPost("messages")]
        public async Task<IResult> Send(SendRequest request
            , CancellationToken cancellationToken)
        {
            await _channel.Writer.WriteAsync(request, cancellationToken);
            return Results.Ok();
        }
    }
}
