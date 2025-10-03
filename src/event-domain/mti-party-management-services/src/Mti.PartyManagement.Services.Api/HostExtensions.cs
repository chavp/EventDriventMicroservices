using Microsoft.EntityFrameworkCore;
using Mti.PartyManagement.Persistence;
using Mti.PartyManagement.Services.Api.Infrastructure;

namespace Mti.PartyManagement.Services.Api
{
    public static class HostExtensions
    {
        public static async Task SeedDatabaseAsync(this IHost host)
        {
            using (var scope = host.Services.CreateScope())
            {
                var services = scope.ServiceProvider;
                try
                {
                    IDbContextFactory<PartiesContext> dbFactory = services.GetRequiredService<IDbContextFactory<PartiesContext>>();
                    using (var context = dbFactory.CreateDbContext())
                    {
                        // Ensure the database is created
                        context.Database.Migrate();
                    }

                    var partiesSeeder = new PartiesSeeder(dbFactory);
                    partiesSeeder.Seed();
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
            await TopicExtensions.CreateKafkaTopicAsync(
                host.Configuration["Kafka:BootstrapServers"],
                host.Configuration["Kafka:SavePartiesByOrder:RequestTopic"]);

            await TopicExtensions.CreateKafkaTopicAsync(
                host.Configuration["Kafka:BootstrapServers"],
                host.Configuration["Kafka:SavePartiesByOrder:ResponseTopic"]);

        }
    }
}
