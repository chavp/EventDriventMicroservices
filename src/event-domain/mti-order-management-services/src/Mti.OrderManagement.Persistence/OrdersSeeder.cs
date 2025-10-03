using Microsoft.EntityFrameworkCore;
using Mti.OrderManagement.Domain.Orders.Types;

namespace Mti.OrderManagement.Persistence
{
    public class OrdersSeeder
    {
        protected readonly IDbContextFactory<OrdersContext> _dbFacto = null;
        public OrdersSeeder(IDbContextFactory<OrdersContext> dbFacto)
        {
            _dbFacto = dbFacto;
        }

        public void Seed()
        {
            using (var db = _dbFacto.CreateDbContext())
            {
                // seed org title
                saveOrderRoleType(db, OrderRoleType.Insured);
                saveOrderRoleType(db, OrderRoleType.Invoice);

                saveOrderRoleType(db, OrderRoleType.Owner);

                db.SaveChanges();
            }
        }

        private void saveOrderRoleType(OrdersContext db, string code)
        {
            if (!db.OrderRoleTypes.Any(x => x.Code == code))
                db.Add(new OrderRoleType(code) { });
        }
    }
}
