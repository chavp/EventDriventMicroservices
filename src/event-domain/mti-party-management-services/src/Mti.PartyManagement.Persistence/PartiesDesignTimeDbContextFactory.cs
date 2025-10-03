using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Mti.PartyManagement.Persistence
{
    public class PartiesDesignTimeDbContextFactory : IDesignTimeDbContextFactory<PartiesContext>
    {
        public PartiesContext CreateDbContext(string[] args)
        {
            var optionsBuilder = new DbContextOptionsBuilder<PartiesContext>();
            optionsBuilder.UseNpgsql("Server=localhost;TrustServerCertificate=True;User Id=postgres;Password=animalfarm888");
            return new PartiesContext(optionsBuilder.Options);
        }
    }
}
