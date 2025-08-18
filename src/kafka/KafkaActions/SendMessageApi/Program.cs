using System.Threading.Channels;
using Send.Messaging.Send;
using SendMessageApi.BackgroundServices;
using SendMessageApi.BackgroundTasks;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddHostedService<SendProducerBackgroundService>();
builder.Services.AddHostedService<SendProcessorBackgroundService>();

builder.Services.AddSingleton(_ => Channel.CreateUnbounded<SendRequest>(new UnboundedChannelOptions
{
    SingleReader = true,
    AllowSynchronousContinuations = false,
}));

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
