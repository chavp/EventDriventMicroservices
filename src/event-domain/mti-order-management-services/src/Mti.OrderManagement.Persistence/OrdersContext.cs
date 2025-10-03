using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Mti.Domain.Application.Abstractions.Common;
using Mti.Domain.Application.Abstractions.Data;
using Mti.Domain.Core.Abstractions;
using Mti.OrderManagement.Domain.Orders;
using Mti.OrderManagement.Domain.Orders.Types;
using Mti.OrderManagement.Persistence.Extensions;

namespace Mti.OrderManagement.Persistence
{
    public class OrdersContext : DbContext, IUnitOfWork
    {
        // Types
        public DbSet<OrderRoleType> OrderRoleTypes { get; set; }

        // Transactions
        public DbSet<Order> Orders { get; set; }
        public DbSet<OrderItem> OrderItems { get; set; }
        public DbSet<InsuredAsset> InsuredAssets { get; set; }


        public OrdersContext(DbContextOptions<OrdersContext> options)
            : base(options)
        {

        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasDefaultSchema("orders");

            modelBuilder.ApplyUpperConverter(["Code", "OrderNumber"]);

            modelBuilder.Entity<Order>();
            modelBuilder.Entity<SalesOrder>();
            modelBuilder.Entity<MtiOriginalSalesOrder>();

            modelBuilder.Entity<OrderItem>();
            modelBuilder.Entity<SalesOrderItem>();
            modelBuilder.Entity<MtiOriginalSalesOrderItem>();
        }

        private async Task saveChangeAsync(CancellationToken cancellationToken = default)
        {
            updateAuditableEntities(DateTime.UtcNow);

            //UpdateSoftDeletableEntities(utcNow);

            //await publishDomainEvents(cancellationToken);
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
            foreach (EntityEntry<IAuditableEntity> entityEntry in ChangeTracker.Entries<IAuditableEntity>())
            {
                if (entityEntry.State == EntityState.Added)
                {
                    entityEntry.Property(nameof(IAuditableEntity.CreatedOnUtc)).CurrentValue = utcNow;
                }

                if (entityEntry.State == EntityState.Modified)
                {
                    entityEntry.Property(nameof(IAuditableEntity.ModifiedOnUtc)).CurrentValue = utcNow;

                    var revCurrentValue = entityEntry.Property(nameof(IAuditableEntity.Revision)).CurrentValue;
                    entityEntry.Property(nameof(IAuditableEntity.Revision)).CurrentValue = Convert.ToUInt32(revCurrentValue) + 1;
                }
            }
        }

        public async Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
            => await Database.BeginTransactionAsync(cancellationToken);
    }
}
