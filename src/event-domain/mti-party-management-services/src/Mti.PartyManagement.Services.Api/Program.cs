using Microsoft.EntityFrameworkCore;
using Mti.PartyManagement.Persistence;
using Mti.PartyManagement.Services.Api;
using Mti.PartyManagement.Services.Api.Applications.GetAssetsByIds;
using Mti.PartyManagement.Services.Api.BackgroundServices.Parties;
using Mti.PartyManagement.Services.Api.BackgroundServices.SavePartiesByOrder;
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


builder.Services.AddTransient<IDbContextFactory<PartiesContext>, PartiesDbContextFactory>(
    x => new PartiesDbContextFactory(
            builder.Configuration.GetConnectionString("PartiesDb"))
);

builder.Services.AddSingleton<GetAssetsByIdsHandler>();

builder.Services.AddHostedService<SavePartiesByOrderConsumer>();
builder.Services.AddHostedService<PartyService>();

var app = builder.Build();

await app.SeedDatabaseAsync();
//await app.CreateTopicAsync();

app.Logger.LogInformation("Starting Mti.PartyManagement.Services.Api...");

app.Run();

app.Logger.LogInformation("Mti.PartyManagement.Services.Api stoped successfully.");
