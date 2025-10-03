using Microsoft.EntityFrameworkCore;

namespace Mti.OrderQueries.Persistence
{
    public class OrderQueriesDbContextFactory : IDbContextFactory<OrderQueriesContext>
    {
        private DbContextOptions<OrderQueriesContext> _options;
        public OrderQueriesDbContextFactory(string connectionString)
        {
            _options = new DbContextOptionsBuilder<OrderQueriesContext>()
                .UseNpgsql(connectionString)
                .Options;
        }

        public OrderQueriesContext CreateDbContext()
        {
            return new OrderQueriesContext(_options);
        }
    }
}
