using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Mti.ProductManagement.Persistance;
using Newtonsoft.Json;
using Serilog;
using Xunit.Abstractions;

namespace Mti.ProductManagement.Domain
{
    /// <summary>
    /// docker network create -d bridge mti_insurance_platform
    /// docker run --name some-postgres -e POSTGRES_PASSWORD=animalfarm888 -d -p 5432:5432 postgres
    ///
    /// </summary>
    public abstract class DbTestBase
    {
        protected readonly ITestOutputHelper _testOutputHelper;

        protected readonly IConfigurationRoot _config = null;
        protected readonly IDbContextFactory<ProductsContext> _productsFac = null;

        protected readonly LoggerFactory loggerFactory = new LoggerFactory();
        public DbTestBase(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;

            _config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();

            _productsFac = new ProductsDbContextFactory(_config.GetConnectionString("products"));

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug() // Set the minimum logging level
                .WriteTo.Console() // Configure console logging
                .CreateLogger();

            loggerFactory.AddSerilog(Log.Logger);
        }
    }
}
