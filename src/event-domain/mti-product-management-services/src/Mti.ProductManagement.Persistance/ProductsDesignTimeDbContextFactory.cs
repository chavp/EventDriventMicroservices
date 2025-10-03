using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Mti.ProductManagement.Persistance
{
    public class ProductsDesignTimeDbContextFactory : IDesignTimeDbContextFactory<ProductsContext>
    {
        public ProductsContext CreateDbContext(string[] args)
        {
            var optionsBuilder = new DbContextOptionsBuilder<ProductsContext>();
            optionsBuilder.UseNpgsql("Server=localhost;TrustServerCertificate=True;User Id=postgres;Password=animalfarm888");
            return new ProductsContext(optionsBuilder.Options);
        }
    }
}
