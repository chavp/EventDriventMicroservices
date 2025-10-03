using Microsoft.EntityFrameworkCore;
using Mti.ProductManagement.Domain;
using Mti.ProductManagement.Domain.Products;

namespace Mti.ProductManagement.Persistance
{
    public class ProductsSeeder
    {
        protected readonly IDbContextFactory<ProductsContext> _dbFacto = null;
        public ProductsSeeder(IDbContextFactory<ProductsContext> dbFacto)
        {
            _dbFacto = dbFacto;
        }

        public void SeedInit()
        {
            using (var prdDb = _dbFacto.CreateDbContext())
            using (var tran = prdDb.Database.BeginTransaction())
            {
                var motor = saveProductCatogory(prdDb, ProductCategory.Motor);
                var mtiOrig = saveProductCatogory(prdDb, ProductCategory.MtiOriginal);
                saveParentProductCatogory(prdDb, motor.Id.Value, mtiOrig.Id.Value);

                saveType<CoverageType>(prdDb, CoverageType.SumInsured);
                saveType<CoverageType>(prdDb, CoverageType.Deductible);
                saveType<CoverageType>(prdDb, CoverageType.DamageLife);
                saveType<CoverageType>(prdDb, CoverageType.DamageInsure);
                saveType<CoverageType>(prdDb, CoverageType.Accident);
                saveType<CoverageType>(prdDb, CoverageType.MedicalInsure);
                saveType<CoverageType>(prdDb, CoverageType.InsureDriver);

                saveType<CoverageLevelBasis>(prdDb, CoverageLevelBasis.PerIncident);
                saveType<CoverageLevelBasis>(prdDb, CoverageLevelBasis.PerPerson);
                saveType<CoverageLevelBasis>(prdDb, CoverageLevelBasis.PerDisablitity);
                saveType<CoverageLevelBasis>(prdDb, CoverageLevelBasis.PerDriver);
                saveType<CoverageLevelBasis>(prdDb, CoverageLevelBasis.PerTime);

                saveType<CoverageLevelType>(prdDb, CoverageLevelType.CoverageAmount);
                saveType<CoverageLevelType>(prdDb, CoverageLevelType.Deductibility);

                saveType<ProductFeatureType>(prdDb, ProductFeatureType.VehicleCode);
                saveType<ProductFeatureType>(prdDb, ProductFeatureType.VehicleBrand);
                saveType<ProductFeatureType>(prdDb, ProductFeatureType.VehicleModel);
                saveType<ProductFeatureType>(prdDb, ProductFeatureType.VehicleYear);

                
                tran.Commit();
            }
        }

        private ProductCategory saveProductCatogory(ProductsContext prdDb, string code)
        {
            var cat = saveType<ProductCategory>(prdDb, code);
            //var cat = prdDb.ProductCatogories.SingleOrDefault(x => x.Code == code);
            //if (cat == null)
            //{
            //    cat = new ProductCatogory(code);
            //    prdDb.Add(cat);
            //}

            //prdDb.SaveChanges();
            return cat;
        }
        private void saveParentProductCatogory(ProductsContext prdDb, Guid parentId, Guid childId)
        {
            var parent = prdDb.ProductCatogories.Single(x => x.Id == parentId);
            var child = prdDb.ProductCatogories.Single(x => x.Id == childId);
            child.ParentProductCategoryId = parent.Id;

            prdDb.SaveChanges();
        }

        private T saveType<T>(ProductsContext prdDb, string code)
            where T : TypeModel, new()
        {
            var t = prdDb.Set<T>().SingleOrDefault(x => x.Code == code);
            if (t == null)
            {
                t = new T { Code = code };
                prdDb.Add(t);
            }

            prdDb.SaveChanges();
            return t;
        }
    }
}
