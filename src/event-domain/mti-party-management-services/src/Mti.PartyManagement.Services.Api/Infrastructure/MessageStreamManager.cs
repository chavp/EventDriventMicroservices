using System.Net;
using System.Text;
using Confluent.Kafka;
using FluentResults;
using Mti.PartyManagement.Messaging.Parties.GetAssetsByIds;
using Newtonsoft.Json;

namespace Mti.PartyManagement.Services.Api.Infrastructure
{
    public class MessageStreamManager<TRequest, TResponse>
    {
        private readonly ILogger _logger;
        private readonly string _bootstrapServers;
        private readonly string _groupId;
        public MessageStreamManager(ILogger logger,
            string bootstrapServers,
            string groupId) 
        { 
            _logger = logger;
            _bootstrapServers = bootstrapServers;
            _groupId = groupId;
        }

        public ConsumerBuilder<string, TRequest> ConsumerBuilder()
        {
            var consumerBuilder = new ConsumerBuilder<string, TRequest>(new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            })
            .SetErrorHandler((_, e) => _logger.LogError($"Error in consumer: {e.Reason}"))
            .SetValueDeserializer(new JsonDeserializer<TRequest>())
            ;
            return consumerBuilder;
        }

        public ProducerBuilder<string, TResponse> ProducerBuilder()
        {
            var producerBuilder = new ProducerBuilder<string, TResponse>(new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                ClientId = Dns.GetHostName(),
                EnableIdempotence = true,
                Acks = Acks.All,
            })
            .SetErrorHandler((_, e) => _logger.LogError($"Error in producer: {e.Reason}"))
            .SetValueSerializer(new JsonSerializer<TResponse>())
            ;
            return producerBuilder;
        }

        public async Task<Result> ProduceResponseAsync<TResponse>(
            string correlationId,
            string replyToTopic,
            TResponse response,
            CancellationToken cancellationToken)
        {
            try
            {
                var producerBuilder = new ProducerBuilder<string, TResponse>(new ProducerConfig
                {
                    BootstrapServers = _bootstrapServers,
                    ClientId = Dns.GetHostName(),
                    EnableIdempotence = true,
                    Acks = Acks.All,
                })
                .SetErrorHandler((_, e) => _logger.LogError($"Error in producer: {e.Reason}"))
                .SetValueSerializer(new JsonSerializer<TResponse>());

                _logger.LogDebug("Producing response for: {resp}", JsonConvert.SerializeObject(response));

                //throw new NotImplementedException("Test Error");
                var message = new Message<string, TResponse>
                {
                    Key = correlationId,
                    Value = response,
                    Headers = new Headers
                    {
                        { "CorrelationId", Encoding.UTF8.GetBytes(correlationId) },
                        { "ReplyTo", Encoding.UTF8.GetBytes(replyToTopic) }
                    }
                };

                using (var producer = producerBuilder.Build())
                {
                    var deliveryResult = await producer
                            .ProduceAsync(replyToTopic, message, cancellationToken);
                    _logger.LogInformation($"Response correlationId = {correlationId}, reply = {replyToTopic}, correlationId = {correlationId}");
                }

                return Result.Ok();
            }
            catch (ProduceException<Guid, TResponse> ex)
            {
                _logger.LogError(ex, "Failed to send message");
                return Result.Fail(new FluentResults.Error("Failed to send message")
                    .CausedBy(ex));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An unexpected error occurred while sending message");
                return Result.Fail(new FluentResults.Error("An unexpected error occurred while sending message")
                    .CausedBy(ex));
            }

        }

        public async Task<Result> ProduceErrorResponseAsync<TResponse>(
            string correlationId,
            string replyToTopic,
            IError error,
            CancellationToken cancellationToken)
            where TResponse : new()
        {
            var producerBuilder = new ProducerBuilder<string, TResponse>(new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                ClientId = Dns.GetHostName(),
                EnableIdempotence = true,
                Acks = Acks.All,
            })
            .SetValueSerializer(new JsonSerializer<TResponse>());

            try
            {
                var errorTitle = error.Message;
                var errorMessage = string.Join(",", error.Reasons.Select(x => x.Message));
                var message = new Message<string, TResponse>
                {
                    Key = correlationId,
                    Value = new TResponse(),
                    Headers = new Headers
                    {
                        { "CorrelationId", Encoding.UTF8.GetBytes(correlationId) },
                        { "Title", Encoding.UTF8.GetBytes(errorTitle) },
                        { "Error", Encoding.UTF8.GetBytes(errorMessage) },
                    }
                };
                using (var producer = producerBuilder.Build())
                {
                    var deliveryResult = await producer
                            .ProduceAsync(replyToTopic, message, cancellationToken);
                    _logger.LogInformation($"Response error correlationId = {correlationId}");
                }
            }
            catch (ProduceException<Guid, TResponse> ex)
            {
                _logger.LogError(ex, "Failed to send message");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An unexpected error occurred while sending message");
            }
            return Result.Ok();
        }
    }
}
