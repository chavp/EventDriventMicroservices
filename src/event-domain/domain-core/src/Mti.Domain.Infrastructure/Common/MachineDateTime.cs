using System;
using Mti.Domain.Application.Abstractions.Common;

namespace Mti.Domain.Infrastructure.Common
{
    /// <summary>
    /// Represents the machine date time service.
    /// </summary>
    public sealed class MachineDateTime : IDateTime
    {
        /// <inheritdoc />
        public DateTime UtcNow => DateTime.UtcNow;
    }
}
