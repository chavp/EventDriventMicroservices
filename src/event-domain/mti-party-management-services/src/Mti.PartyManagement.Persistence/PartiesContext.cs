using Microsoft.EntityFrameworkCore;

namespace Mti.PartyManagement.Persistence
{
    using Microsoft.EntityFrameworkCore.ChangeTracking;
    using Microsoft.EntityFrameworkCore.Storage;
    using Mti.PartyManagement.Domain;
    using Mti.PartyManagement.Domain.Parties;
    using Mti.PartyManagement.Domain.Parties.Types;
    using Mti.PartyManagement.Persistence.Extensions;

    public class PartiesContext : DbContext
    {
        // Types
        public DbSet<PartyTitle> PartyTitles { get; set; }
        public DbSet<Nationality> Nationalities { get; set; }
        public DbSet<AssetRoleType> AssetRoleTypes { get; set; }
        public DbSet<ContactMechanismType> ContactMechanismTypes { get; set; }

        // Transactions
        public DbSet<Party> Parties { get; set; }
        public DbSet<Asset> Assets { get; set; }
        public DbSet<ContactMechanism> ContactMechanisms { get; set; }

        public DbSet<PartyRole> PartyRoles { get; set; }
        public DbSet<AgentChannel> AgentChannels { get; set; }
        public DbSet<AgentMaster> AgentMasters { get; set; }

        public DbSet<PartyRoleType> PartyRoleTypes { get; set; }

        public PartiesContext(DbContextOptions<PartiesContext> options)
            : base(options)
        {
            
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasDefaultSchema("parties");

            modelBuilder.Entity<Party>();
            modelBuilder.Entity<Person>();
            modelBuilder.Entity<Organization>();
            modelBuilder.Entity<LegalOrganization>();
            modelBuilder.Entity<InformalOrganization>();

            modelBuilder.Entity<Asset>();
            modelBuilder.Entity<Vehicle>();

            modelBuilder.Entity<ContactMechanism>();
            modelBuilder.Entity<PostalAddresse>();

            modelBuilder.Entity<PartyRole>();
            modelBuilder.Entity<Agent>();
            modelBuilder.Entity<Invoice>();
            modelBuilder.Entity<InsuredParty>();
            modelBuilder.Entity<Insured>();

            modelBuilder.ApplyUpperConverter(["Code"]);

            modelBuilder.Entity<Party>()
                .HasMany(e => e.ContactMechanisms)
                .WithMany(e => e.Parties)
                .UsingEntity<PartyContactMechanism>(
                    r => r.HasOne(e => e.ContactMechanism).WithMany(x => x.PartyContactMechanisms),
                    l => l.HasOne(e => e.Party).WithMany(x => x.PartyContactMechanisms)
                )
                ;
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

        public async Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
            => await Database.BeginTransactionAsync(cancellationToken);
    }
}
