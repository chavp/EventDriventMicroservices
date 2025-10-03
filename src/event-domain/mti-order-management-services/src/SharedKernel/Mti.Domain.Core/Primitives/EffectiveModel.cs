using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Primitives;

namespace Mti.Domain.Core.Primitives
{
    public abstract class EffectiveModel : Entity
    {
        public DateOnly EffectiveDate { get; set; } = DateOnly.FromDateTime(DateTime.Now);
        public DateOnly ExpiryDate { get; set; } = DateOnly.FromDateTime(DateTime.MaxValue);

        public void Expire(DateOnly? expiryDate = null, string? lastModifiedBy = null)
        {
            ExpiryDate = expiryDate ?? DateOnly.FromDateTime(DateTime.Today.AddDays(-1));
        }
    }
}
