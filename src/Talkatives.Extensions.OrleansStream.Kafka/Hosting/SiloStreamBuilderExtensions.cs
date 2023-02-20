using Confluent.Kafka;
using Talkatives.Extensions.OrleansStream.Kafka.Distributors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streams;
using Talkatives.Extensions.OrleansStream.Kafka.Providers;
using Talkatives.Extensions.OrleansStreams.Abstractions;

namespace Talkatives.Extensions.OrleansStream.Kafka.Hosting
{
    public static class SiloStreamBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use azure queue persistent streams.
        /// </summary>
        public static IHostBuilder AddKafkaTopicStreams<TEvent>(this IHostBuilder builder, string name,
            Action<SiloKafkaTopicStreamConfigurator> configure) where TEvent : IMessageBody
        {
            return AddKafkaTopicStreams(builder, name, configure);
        }

        public static IHostBuilder AddKafkaTopicStreams<TEvent1, TEvent2>(this IHostBuilder builder, string name,
                Action<SiloKafkaTopicStreamConfigurator> configure) where TEvent1 : IMessageBody
                                                                    where TEvent2 : IMessageBody
        {
            return AddKafkaTopicStreams(builder, name, configure);
        }

        public static IHostBuilder AddKafkaTopicStreams<TEvent1, TEvent2, TEvent3>(this IHostBuilder builder, string name,
                Action<SiloKafkaTopicStreamConfigurator> configure) where TEvent1 : IMessageBody
                                                                    where TEvent2 : IMessageBody
                                                                    where TEvent3 : IMessageBody
        {
            return AddKafkaTopicStreams(builder, name, configure);
        }

        public static IHostBuilder AddKafkaTopicStreams<TEvent1, TEvent2, TEvent3, TEvent4>(this IHostBuilder builder, string name,
                Action<SiloKafkaTopicStreamConfigurator> configure) where TEvent1 : IMessageBody
                                                                    where TEvent2 : IMessageBody
                                                                    where TEvent3 : IMessageBody
                                                                    where TEvent4 : IMessageBody
        {
            return AddKafkaTopicStreams(builder, name, configure);
        }

        private static IHostBuilder AddKafkaTopicStreams(IHostBuilder builder, string name,
                    Action<SiloKafkaTopicStreamConfigurator> configure)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentOutOfRangeException("name", "cannot be a null value");
            }
            builder.ConfigureServices((ctx, services) =>
            {
                var basePath = $"OrleansServerConfiguration:Streams:{name}";
                services.Configure<ConsumerConfig>(name, ctx.Configuration.GetSection($"{basePath}:ConsumerConfig"));
                services.Configure<ProducerConfig>(name, ctx.Configuration.GetSection($"{basePath}:ProducerConfig"));
                services.Configure<KafkaTopicOptions>(name, ctx.Configuration.GetSection($"{basePath}:KafkaTopicOptions"));
                services.Configure<SimpleQueueCacheOptions>(name, ctx.Configuration.GetSection($"{basePath}:QueueCacheOptions"));

                services.AddSingletonNamedService<IQueueDataAdapter<string, IBatchContainer>, KafkaTopicDataAdapter>(name);
                services.AddSingletonNamedService<IStreamQueueMapper>(name, (sp, name) =>
                {
                    var ktOptions = sp.GetOptionsByName<KafkaTopicOptions>(name);
                    return new HashRingBasedPartitionedStreamQueueMapper(ktOptions.GetPartitionIds(), name);
                });
                services.AddSingletonNamedService<KafkaMessageDistributor>(name, (sp, name) =>
                {
                    var cConfig = sp.GetOptionsByName<ConsumerConfig>(name);
                    var pConfig = sp.GetOptionsByName<ProducerConfig>(name);
                    var qMapper = sp.GetRequiredServiceByName<IStreamQueueMapper>(name);
                    var logger = sp.GetRequiredService<ILogger<KafkaMessageDistributor>>();

                    return new KafkaMessageDistributor(name, cConfig, pConfig, qMapper, null, logger);
                });
            });

            var configurator = new SiloKafkaTopicStreamConfigurator(name,
                configureServicesDelegate => builder.UseOrleans(sBuilder => sBuilder.ConfigureServices(configureServicesDelegate)));
            configure?.Invoke(configurator);
            return builder;
        }
    }
}