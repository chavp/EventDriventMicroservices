using System.ComponentModel.DataAnnotations;

namespace Mti.PartyManagement.Domain
{
    public abstract class Entity
    {
        public Guid? Id { get; set; }

        public DateTime CreatedOnUtc { get; set; } = DateTime.UtcNow;

        [StringLength(300)]
        public string CreatedBy { get; set; } = Environment.MachineName;
        public DateTime? ModifiedOnUtc { get; set; }

        [StringLength(300)]
        public string? ModifiedBy { get; set; }

        public uint Revision { get; protected set; }

        //public void Update(string? updateBy = null)
        //{
        //    LastModifiedBy = updateBy ?? Environment.MachineName;
        //    LastModifiedDateUtc = DateTime.UtcNow;
        //    ++Revision;
        //}
    }
}
