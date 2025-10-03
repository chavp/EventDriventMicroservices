using FluentAssertions;
using Microsoft.Extensions.Logging;
using Mti.ProductManagement.Application.Products.SaveProductMtiOriginal;
using Mti.ProductManagement.Messaging.Products.Commands;
using Mti.ProductManagement.Persistance;
using Xunit.Abstractions;

namespace Mti.ProductManagement.Domain.Tests.Integration
{
    public class ProductTests : DbTestBase, IDisposable
    {

        public ProductTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public void Seed_data()
        {
            var num = Math.Clamp(-1, 1, 100);

            var seeder = new ProductsSeeder(_productsFac);
            seeder.SeedInit();
        }

        [Theory]
        [MemberData(nameof(GetProductTest))]
        public async Task SaveProduct_with_no_dup(string code, string name)
        {
            var request = new SaveProductsByOrderRequest(Guid.NewGuid())
            {
                OrderItems = new List<SaveProductByOrderItemRequest>
                {
                    new SaveProductByOrderItemRequest(Guid.NewGuid())
                    {
                        Product = new ProductRequest(code, name)
                    }
                }
            };

            var saveProductCommand = 
                new SaveProductMtiOriginalCommand(request);

            var saveProductCommandHandler = 
                new SaveProductMtiOriginalCommandHandler(
                    loggerFactory.CreateLogger<SaveProductMtiOriginalCommandHandler>(), _productsFac);

            var result = await saveProductCommandHandler
                .Handle(saveProductCommand, CancellationToken.None);

            result.IsSuccess.Should().BeTrue();
        }

        [Theory]
        [InlineData(null, null, "ProductCode")]
        [InlineData("", null, "ProductCode")]
        [InlineData("555", null, "ProductName")]
        [InlineData("555", "", "ProductName")]
        public async Task SaveProduct_with_error(string? code, string? name, string expected)
        {
            var request = new SaveProductsByOrderRequest(Guid.NewGuid())
            {
                OrderItems = new List<SaveProductByOrderItemRequest>
                {
                    new SaveProductByOrderItemRequest(Guid.NewGuid())
                    {
                        Product = new ProductRequest(code, name)
                    }
                }
            };

            var saveProductCommand =
                new SaveProductMtiOriginalCommand(request);

            var saveProductCommandHandler =
                new SaveProductMtiOriginalCommandHandler(
                    loggerFactory.CreateLogger<SaveProductMtiOriginalCommandHandler>(), _productsFac);

            var result = await saveProductCommandHandler
                .Handle(saveProductCommand, CancellationToken.None);

            result.IsFailed.Should().BeTrue();
            result.Reasons.Should().ContainSingle();
            result.Reasons.Should().Contain(r => r.Message.Contains(expected));
        }

        public static IEnumerable<object[]> GetProductTest()
        {
            yield return new object[] { "Test Product", "TEST123" };
            yield return new object[] { "Test Product2", "TEST123" };
            yield return new object[] { "Test Product3", "TEST125" };
        }

        public void Dispose()
        {
            using(var db = _productsFac.CreateDbContext())
            {
                var products = GetProductTest();
                foreach (var item in products)
                {
                    var del = db.Products.SingleOrDefault(x => x.Code == item[0]);
                    if (del != null)
                    {
                        db.Products.Remove(del);
                        db.SaveChanges();
                    }
                }
            }
        }
    }
}