using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.ProductManagement.Domain
{
    public abstract class EffectiveModel : Entity
    {
        public DateOnly EffectiveDate { get; set; } = DateOnly.FromDateTime(DateTime.Now);
        public DateOnly ExpiryDate { get; set; } = DateOnly.FromDateTime(DateTime.MaxValue);

        public void Expire(DateOnly? expiryDate = null, string? updateBy = null)
        {
            ExpiryDate = expiryDate ?? DateOnly.FromDateTime(DateTime.Today.AddDays(-1));
            updateBy = updateBy ?? Environment.MachineName;
        }
    }
}
