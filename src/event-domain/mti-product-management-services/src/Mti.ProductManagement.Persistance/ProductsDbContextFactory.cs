using Microsoft.EntityFrameworkCore;

namespace Mti.ProductManagement.Persistance
{
    public class ProductsDbContextFactory : IDbContextFactory<ProductsContext>
    {
        private DbContextOptions<ProductsContext> _options;

        public ProductsDbContextFactory(string connectionString)
        {
            _options = new DbContextOptionsBuilder<ProductsContext>()
                .UseNpgsql(connectionString)
                .Options;
        }

        public ProductsContext CreateDbContext()
        {
            return new ProductsContext(_options);
        }
    }
}
