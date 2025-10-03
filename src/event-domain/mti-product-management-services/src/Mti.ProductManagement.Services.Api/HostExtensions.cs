using Microsoft.EntityFrameworkCore;
using Mti.ProductManagement.Persistance;
using Mti.ProductManagement.Services.Api.Infrastructure;

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
                    IDbContextFactory<ProductsContext> dbFactory = services.GetRequiredService<IDbContextFactory<ProductsContext>>();
                    using (var context = dbFactory.CreateDbContext())
                    {
                        // Ensure the database is created
                        await context.Database.MigrateAsync();
                    }

                    var seeder = new ProductsSeeder(dbFactory);
                    seeder.SeedInit();
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
            //    [host.Configuration["Kafka:SaveProductMtiOriginal:RequestTopic"]
            //    , host.Configuration["Kafka:SaveProductMtiOriginal:ResponseTopic"]]);

            await TopicExtensions.CreateKafkaTopicAsync(
                host.Configuration["Kafka:BootstrapServers"],
                host.Configuration["Kafka:SaveProductsByOrderService:RequestTopic"]);

            await TopicExtensions.CreateKafkaTopicAsync(
                host.Configuration["Kafka:BootstrapServers"],
                host.Configuration["Kafka:SaveProductsByOrderService:ResponseTopic"]);

        }
    }
}
