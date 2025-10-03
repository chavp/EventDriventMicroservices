using MediatR;
using Microsoft.EntityFrameworkCore;
using Mti.Domain.Application.Abstractions.Common;

namespace Mti.OrderManagement.Persistence
{
    public class OrdersDbContextFactory : IDbContextFactory<OrdersContext>
    {
        private DbContextOptions<OrdersContext> _options;

        public OrdersDbContextFactory(string connectionString)
        {
            _options = new DbContextOptionsBuilder<OrdersContext>()
                .UseNpgsql(connectionString)
                .Options;
        }

        public OrdersContext CreateDbContext()
        {
            return new OrdersContext(_options);
        }
    }
}
