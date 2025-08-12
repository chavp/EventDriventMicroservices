using BackgroundServicesDemo.BackgroundServices;
using BackgroundServicesDemo.Contracts;
using System.Threading.Channels;

var builder = WebApplication.CreateBuilder(args);
//builder.Logging.ClearProviders();
//builder.Logging.AddConsole();

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

//builder.Services.AddHostedService<SampleService1>();
//builder.Services.AddHostedService<SampleService2>();
builder.Services.AddSingleton<Channel<FileImportRequest>>(c => Channel.CreateUnbounded<FileImportRequest>());
builder.Services.AddHostedService<FileImportService>();

var app = builder.Build();

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
