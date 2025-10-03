using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;


namespace Mti.ProductManagement.Persistance
{
    using Mti.ProductManagement.Domain;
    using Mti.ProductManagement.Domain.Products;
    using Mti.ProductManagement.Persistence.Extensions;

    public class ProductsContext : DbContext
    {
        // Types
        public DbSet<CoverageLevelType> CoverageLevelTypes { get; set; }
        public DbSet<ProductCategory> ProductCatogories { get; set; }
        public DbSet<CoverageType> CoverageTypes { get; set; }
        public DbSet<CoverageLevelBasis> CoverageLevelBasises { get; set; }

        // Transactions
        public DbSet<Product> Products { get; set; }
        public DbSet<CoverageLevel> CoverageLevels { get; set; }
        public DbSet<CoverageAvailability> CoverageAvailabilities { get; set; }
        public DbSet<ProductFeatureType> ProductFeatureTypes { get; set; }
        public DbSet<ProductFeature> ProductFeatures { get; set; }
        public DbSet<ProductFeatureAvailability> ProductFeatureAvailabilities { get; set; }

        public ProductsContext(DbContextOptions<ProductsContext> options)
            : base(options)
        {

        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasDefaultSchema("products");

            modelBuilder.ApplyUpperConverter(["Code"]);

            modelBuilder.Entity<Product>();

            modelBuilder.Entity<CoverageLevel>();
            modelBuilder.Entity<CoverageAmount>();
            modelBuilder.Entity<Deductibility>();

            modelBuilder.Entity<CoverageAvailability>();
            modelBuilder.Entity<SelectableCoverage>();
            modelBuilder.Entity<OptionalCoverage>();

            modelBuilder.Entity<ProductFeature>();
            modelBuilder.Entity<VehicleCode>();
            modelBuilder.Entity<VehicleBrand>();
            modelBuilder.Entity<VehicleModel>();
            modelBuilder.Entity<VehicleYear>();

            modelBuilder.Entity<ProductFeatureAvailability>();
            modelBuilder.Entity<SelectableFeature>();

            modelBuilder.Entity<ProductCategory>()
                .HasMany(e => e.Products)
                .WithMany(e => e.ProductCatogories)
                .UsingEntity<ProductCategoryClassification>(
                    r => r.HasOne(e => e.Product).WithMany(x => x.ProductCatogoryClassifications),
                    l => l.HasOne(e => e.ProductCatogory).WithMany(x => x.ProductCatogoryClassifications)
                );
        }

        public override int SaveChanges()
        {
            saveChangeAsync().Wait();

            return base.SaveChanges();
        }

        private async Task saveChangeAsync(CancellationToken cancellationToken = default)
        {
            updateAuditableEntities(DateTime.UtcNow);
        }

        private void updateAuditableEntities(DateTime utcNow)
        {
            foreach (EntityEntry<Entity> entityEntry in ChangeTracker.Entries<Entity>())
            {
                if (entityEntry.State == EntityState.Added)
                {
                    entityEntry.Property(nameof(Entity.CreatedOnUtc)).CurrentValue = utcNow;
                }

                if (entityEntry.State == EntityState.Modified)
                {
                    entityEntry.Property(nameof(Entity.ModifiedOnUtc)).CurrentValue = utcNow;

                    var revCurrentValue = entityEntry.Property(nameof(Entity.Revision)).CurrentValue;
                    entityEntry.Property(nameof(Entity.Revision)).CurrentValue = Convert.ToUInt32(revCurrentValue) + 1;
                }
            }
        }
    }
}
