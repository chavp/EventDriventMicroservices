using Microsoft.EntityFrameworkCore;

namespace Mti.OrderQueries.Persistence
{
    using Microsoft.EntityFrameworkCore.ChangeTracking;
    using Microsoft.EntityFrameworkCore.Storage;
    using Mti.OrderQueries.Domain;
    using Mti.OrderQueries.Domain.OrdersWharehouse;

    public class OrderQueriesContext : DbContext
    {
        public DbSet<OrderItemFact> OrderItemFacts { get; set; }
        public DbSet<ApplicationDim> ApplicationDims { get; set; }
        public DbSet<ContactMechanismDim> ContactMechanismDims { get; set; }
        public DbSet<InsuredAssetDim> InsuredAssetDims { get; set; }
        public DbSet<OrderDim> OrderDims { get; set; }
        public DbSet<OrderItemPartyRoleDim> OrderItemPartyRoleDims { get; set; }
        public DbSet<PartyDim> PartyDims { get; set; }
        public DbSet<ProductDim> ProductDims { get; set; }

        public OrderQueriesContext(DbContextOptions<OrderQueriesContext> options)
            : base(options)
        {
            
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasDefaultSchema("order_queries");
        }

        private async Task saveChangeAsync(CancellationToken cancellationToken = default)
        {
            updateAuditableEntities(DateTime.UtcNow);
        }

        public override int SaveChanges()
        {
            saveChangeAsync().Wait();

            return base.SaveChanges();
        }

        public override async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
        {
            await saveChangeAsync(cancellationToken);

            return await base.SaveChangesAsync(cancellationToken);
        }

        private void updateAuditableEntities(DateTime utcNow)
        {
            foreach (EntityEntry<StarModel> entityEntry in ChangeTracker.Entries<StarModel>())
            {
                if (entityEntry.State == EntityState.Added)
                {
                    entityEntry.Property(nameof(StarModel.CreatedOnUtc)).CurrentValue = utcNow;
                }

                if (entityEntry.State == EntityState.Modified)
                {
                    entityEntry.Property(nameof(StarModel.ModifiedOnUtc)).CurrentValue = utcNow;

                    var revCurrentValue = entityEntry.Property(nameof(StarModel.Revision)).CurrentValue;
                    entityEntry.Property(nameof(StarModel.Revision)).CurrentValue = Convert.ToUInt32(revCurrentValue) + 1;
                }
            }
        }

        public async Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
            => await Database.BeginTransactionAsync(cancellationToken);
    }
}
