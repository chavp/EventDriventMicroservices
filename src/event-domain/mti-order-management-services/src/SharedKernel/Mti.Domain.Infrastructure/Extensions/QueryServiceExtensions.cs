using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Mti.Domain.Infrastructure.Common;

namespace Mti.Domain.Infrastructure.Extensions
{
    public static class QueryServiceExtensions
    {
        public static async Task<TResponse?> GetRequestResponseAsync<TRequest, TResponse>(
            this TRequest request,
            string bootstrapServers,
            string topicRequest,
            string topicResponse,
            ILogger logger,
            CancellationToken stoppingToken)
            where TResponse : class
        {
            var messageStreamBuilder = new MessageStreamBuilder(logger)
                .WithBootstrapServers(bootstrapServers);

            using (var producer = messageStreamBuilder.BuildProducer<TRequest>())
            {
                TResponse? resp = null;
                try
                {
                    var correlationId = Guid.NewGuid().ToString();
                    var message = messageStreamBuilder
                        .BuildCorrelationMessage(request, correlationId, topicResponse);

                    var deliveryResult = await producer
                            .ProduceAsync(topicRequest, message, stoppingToken);
                    logger.LogDebug($"Message sent to partition {deliveryResult.Partition} with offset {deliveryResult.Offset}, key correlationId {correlationId}");

                    using (var consumer = messageStreamBuilder.BuildReplyCunsumer<TResponse>())
                    {
                        consumer.Subscribe(topicResponse);
                        //while (!stoppingToken.IsCancellationRequested)
                        //{
                            logger.LogDebug($"Consume correlationId = {correlationId}, reply = {topicResponse}");
                            var consumeResult = consumer.Consume(TimeSpan.FromSeconds(30));
                            if (consumeResult == null
                                || consumeResult.Message == null)
                            {
                                logger.LogWarning("No message received, retrying...");
                                throw new Exception("Consume result null or timeout.");
                            }

                            if (consumeResult.Message.Headers.TryGetLastBytes("CorrelationId", out var corIdBytes))
                            {
                                var receivedCorrelationId = Encoding.UTF8.GetString(corIdBytes);

                                // Send a response back if needed
                                if (receivedCorrelationId == correlationId.ToString())
                                {
                                    if (consumeResult.Message.Headers.TryGetLastBytes("Error", out var headerBytes))
                                    {
                                        var error = Encoding.UTF8.GetString(headerBytes);
                                        if (!string.IsNullOrEmpty(error))
                                        {
                                            throw new Exception($"Error received in response: {error}");
                                        }
                                    }

                                    resp = consumeResult.Message.Value;
                                }
                            }

                        //}
                        consumer.Close();
                    }

                }
                catch (ProduceException<string, TRequest> ex)
                {
                    logger.LogError(ex, "Failed to send message");
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "An unexpected error occurred while sending message");
                    throw;
                }

                return resp;
            }
        }
    }
}
