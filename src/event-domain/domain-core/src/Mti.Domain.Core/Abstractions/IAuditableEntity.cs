using System;

namespace Mti.Domain.Core.Abstractions
{
    /// <summary>
    /// Represents the marker interface for auditable entities.
    /// </summary>
    public interface IAuditableEntity
    {
        /// <summary>
        /// Gets the created on date and time in UTC format.
        /// </summary>
        public DateTime CreatedOnUtc { get; }

        /// <summary>
        /// Gets the modified on date and time in UTC format.
        /// </summary>
        public DateTime? ModifiedOnUtc { get; }

        /// <summary>
        /// Modified incremental for concurrency control
        /// </summary>
        public uint Revision { get; }
    }
}