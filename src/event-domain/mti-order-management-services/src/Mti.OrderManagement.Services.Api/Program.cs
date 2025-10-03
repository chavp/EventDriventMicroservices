using System.Threading.Channels;
using Asp.Versioning;
using FluentValidation;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.EntityFrameworkCore;
using Mti.OrderManagement.Application.Orders.SaveMtiOriginal;
using Mti.OrderManagement.Application.Orders.SavePartiesByOrder;
using Mti.OrderManagement.Application.Orders.SaveProducts;
using Mti.OrderManagement.Application.Orders.TransformMtiOriginal;
using Mti.OrderManagement.Application.Parties.Repositories;
using Mti.OrderManagement.Contracts.Orders;
using Mti.OrderManagement.Persistence;
using Mti.OrderManagement.Persistence.Repositories;
using Mti.OrderManagement.Services.Api;
using Mti.ProductManagement.Messaging.Products.Commands;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Add services to the container.
builder.Services.AddApiVersioning(options =>
{
    options.DefaultApiVersion = new ApiVersion(2);
    options.ReportApiVersions = true;
    options.AssumeDefaultVersionWhenUnspecified = true;
    options.ApiVersionReader = ApiVersionReader.Combine(
        new UrlSegmentApiVersionReader(),
        new HeaderApiVersionReader("X-Api-Version"));
})
.AddMvc() // This is needed for controllers
.AddApiExplorer(options =>
{
    options.GroupNameFormat = "'v'V";
    options.SubstituteApiVersionInUrl = true;
});

builder.Services.AddExceptionHandler<GlobalExceptionHandler>();
builder.Services.AddProblemDetails(options =>
{
    options.CustomizeProblemDetails = context =>
    {
        context.ProblemDetails.Instance =
            $"{context.HttpContext.Request.Method} {context.HttpContext.Request.Path}";

        context.ProblemDetails.Extensions.TryAdd("requestId", context.HttpContext.TraceIdentifier);

        var activity = context.HttpContext.Features.Get<IHttpActivityFeature>()?.Activity;
        context.ProblemDetails.Extensions.TryAdd("traceId", activity?.Id);
    };
});

builder.Services.AddValidatorsFromAssembly(typeof(TranformMtiOriginalRequestValidator).Assembly);
//builder.Services.AddSingleton(_ => Channel.CreateUnbounded<SavePartiesByOrderRequest>(new UnboundedChannelOptions
//{
//    SingleReader = true,
//    AllowSynchronousContinuations = false,
//}));

//builder.Services.AddSingleton(_ => Channel.CreateBounded<SavePartiesByOrderRequest>(new BoundedChannelOptions(1)
//{
//    SingleReader = true,
//    AllowSynchronousContinuations = false,
//    FullMode = BoundedChannelFullMode.Wait,
//}));

builder.Services.AddSingleton(_ => Channel.CreateUnbounded<SaveProductsByOrderRequest>(new UnboundedChannelOptions
{
    SingleReader = true,
    AllowSynchronousContinuations = false,
}));
builder.Services.AddSingleton(_ => Channel.CreateUnbounded<MtiOriginalOrderResponse>(new UnboundedChannelOptions
{
    SingleReader = true,
    AllowSynchronousContinuations = false,
}));

// external repositories
builder.Services.AddSingleton<IPartyRepository, InMemoryPartyRepository>();

// persistence
builder.Services.AddTransient<IDbContextFactory<OrdersContext>, OrdersDbContextFactory>(
    x => new OrdersDbContextFactory(
            builder.Configuration.GetConnectionString("OrdersDb"))
);

// application services
builder.Services.AddScoped<SavePartiesByOrderProducer>();

// background service
//builder.Services.AddHostedService<SavePartiesByOrderProducer>();
//builder.Services.AddHostedService<SavePartiesByOrderConsumer>();
builder.Services.AddHostedService<SaveProductsProducer>();
builder.Services.AddHostedService<SaveProductConsumer>();
builder.Services.AddHostedService<SaveMtiOriginalProducer>();

builder.Services.AddSerilog(); // Integrate Serilog with the host

Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Debug() // Set the minimum logging level
        .WriteTo.Console() // Configure console logging
        .WriteTo.File("logs/log-.txt", 
                fileSizeLimitBytes: 5000000, // 5MB
                rollingInterval: RollingInterval.Day,
                rollOnFileSizeLimit: true) // Configure file logging
        .CreateLogger();

var app = builder.Build();

// Initialize infrastructure
await app.SeedDatabaseAsync();
await app.CreateTopicAsync();

app.UseExceptionHandler();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
