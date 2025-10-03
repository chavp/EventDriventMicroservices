using Microsoft.EntityFrameworkCore;
using Mti.Domain.Infrastructure.Extensions;
using Mti.OrderManagement.Persistence;

namespace Mti.OrderManagement.Services.Api
{
    public static class HostExtensions
    {
        public static async Task SeedDatabaseAsync(this WebApplication host)
        {
            using (var scope = host.Services.CreateScope())
            {
                var services = scope.ServiceProvider;
                try
                {
                    IDbContextFactory<OrdersContext> dbFactory = services.GetRequiredService<IDbContextFactory<OrdersContext>>();
                    using (var context = dbFactory.CreateDbContext())
                    {
                        // Ensure the database is created
                        await context.Database.MigrateAsync();
                    }

                    var seeder = new OrdersSeeder(dbFactory);
                    seeder.Seed();
                }
                catch (Exception ex)
                {
                    var logger = services.GetRequiredService<ILogger<Program>>();
                    logger.LogError(ex, "An error occurred while seeding the database.");
                }
            }
        }

        public static async Task CreateTopicAsync(this WebApplication host)
        {
            //await TopicExtensions.DeleteKafkaTopics(
            //    host.Configuration["Kafka:BootstrapServers"],
            //    [host.Configuration["Kafka:SavePartiesByOrder:ResponseTopic"]
            //    , host.Configuration["Kafka:SavePartiesByOrder:RequestTopic"]]);

            await TopicExtensions.CreateKafkaTopicAsync(
                host.Configuration["Kafka:BootstrapServers"],
                host.Configuration["Kafka:SavePartiesByOrder:Producer:RequestTopic"]);

            await TopicExtensions.CreateKafkaTopicAsync(
                host.Configuration["Kafka:BootstrapServers"],
                host.Configuration["Kafka:SavePartiesByOrder:Producer:ResponseTopic"]);

            await TopicExtensions.CreateKafkaTopicAsync(
                host.Configuration["Kafka:BootstrapServers"],
                host.Configuration["Kafka:SaveProducts:Producer:RequestTopic"]);

            await TopicExtensions.CreateKafkaTopicAsync(
                host.Configuration["Kafka:BootstrapServers"],
                host.Configuration["Kafka:SaveProducts:Consumer:ResponseTopic"]);

            // create state topic
            await TopicExtensions.CreateKafkaTopicAsync(
                host.Configuration["Kafka:BootstrapServers"],
                host.Configuration["Kafka:SaveMtiOriginal:Producer:StateTopic"],
                cleanupPolicy: "compact");
        }
    }
}
