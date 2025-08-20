using ProcessorMessageApi.BackgroundServices;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHostedService<ProcessorBackgroundService>();

var app = builder.Build();

app.Run();
