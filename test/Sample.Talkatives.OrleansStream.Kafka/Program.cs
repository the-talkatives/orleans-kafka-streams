using Orleans.Providers;
using Sample.Talkatives.OrleansStream.Kafka.Grains;
using Talkatives.Extensions.OrleansStream.Kafka.Hosting;

var builder = WebApplication.CreateBuilder(args);

// add orleans
builder.Host.UseOrleans(sb =>
{
    sb.UseLocalhostClustering();
    sb.AddMemoryGrainStorage(ProviderConstants.DEFAULT_PUBSUB_PROVIDER_NAME);
});
builder.Host.AddKafkaTopicStreams<KafkaND>(Constants.KafkaProviderName, config =>
{
    config.Dummy = "testing";
});

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseAuthorization();

app.MapControllers();

app.Run();
