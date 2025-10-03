using Microsoft.EntityFrameworkCore;
using Mti.OrderManagement.Services.Api;
using Mti.ProductManagement.Persistance;
using Mti.ProductManagement.Services.Api.BackgroundServices;
using Serilog;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSerilog(); // Integrate Serilog with the host
Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Debug() // Set the minimum logging level
        .WriteTo.Console() // Configure console logging
        .WriteTo.File("logs/log-.txt",
                fileSizeLimitBytes: 5000000, // 5MB
                rollingInterval: RollingInterval.Day,
                rollOnFileSizeLimit: true) // Configure file logging
        .CreateLogger();
builder.Services.AddSingleton<ILoggerFactory, LoggerFactory>(b =>
{
    var loggerFactory = new LoggerFactory();

    loggerFactory.AddSerilog(Log.Logger);
    return loggerFactory;
});

// persistence
builder.Services.AddTransient<IDbContextFactory<ProductsContext>, ProductsDbContextFactory>(
    x => new ProductsDbContextFactory(builder.Configuration.GetConnectionString("ProductsDb"))
);

// services
builder.Services.AddHostedService<SaveProductsByOrderService>();

var app = builder.Build();

app.UseHttpsRedirection();

await app.SeedDatabaseAsync();
await app.CreateTopicAsync();

app.Run();
