using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Mti.OrderQueries.Persistence
{
    public class OrderQueriesDesignTimeDbContextFactory : IDesignTimeDbContextFactory<OrderQueriesContext>
    {
        public OrderQueriesContext CreateDbContext(string[] args)
        {
            var optionsBuilder = new DbContextOptionsBuilder<OrderQueriesContext>();
            optionsBuilder.UseNpgsql("Server=localhost;TrustServerCertificate=True;User Id=postgres;Password=animalfarm888");
            return new OrderQueriesContext(optionsBuilder.Options);
        }
    }
}
