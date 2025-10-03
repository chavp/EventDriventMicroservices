using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Mti.Domain.Application.Abstractions.Common;

namespace Mti.OrderManagement.Persistence
{
    /// <summary>
    /// docker run --name some-postgres -e POSTGRES_PASSWORD=animalfarm888 -d -p 5432:5432 postgres
    /// </summary>
    public class OrdersDesignTimeDbContextFactory : IDesignTimeDbContextFactory<OrdersContext>
    {
        public OrdersContext CreateDbContext(string[] args)
        {
            var optionsBuilder = new DbContextOptionsBuilder<OrdersContext>();
            optionsBuilder.UseNpgsql("Server=localhost;TrustServerCertificate=True;User Id=postgres;Password=animalfarm888");
            return new OrdersContext(optionsBuilder.Options);
        }
    }
}
