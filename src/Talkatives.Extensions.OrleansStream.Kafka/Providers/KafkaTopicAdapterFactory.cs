using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Orleans.Providers.Streams.Common;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Talkatives.Extensions.OrleansStream.Kafka.Providers
{
    public class KafkaTopicAdapterFactory : IQueueAdapterFactory
    {
        private readonly string _name;
        private readonly IStreamQueueMapper _streamQueueMapper;
        private readonly ILogger<KafkaTopicAdapterFactory> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly SimpleQueueAdapterCache _adapterCache;

        #region .ctor

        public KafkaTopicAdapterFactory(string name,
            IServiceProvider serviceProvider)
        {
            _name = name;
            _serviceProvider = serviceProvider;
            _streamQueueMapper = _serviceProvider.GetRequiredServiceByName<IStreamQueueMapper>(_name);
            _logger = serviceProvider.GetRequiredService<ILogger<KafkaTopicAdapterFactory>>();

            var cacheOptions = _serviceProvider.GetOptionsByName<SimpleQueueCacheOptions>(_name);
            var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();
            _adapterCache = new SimpleQueueAdapterCache(cacheOptions, _name, loggerFactory);
        }

        #endregion

        protected Func<QueueId, Task<IStreamFailureHandler>> StreamFailureHandlerFactory { private get; set; }

        public async Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new KafkaTopicAdapter(_name,
                _serviceProvider);
            return await Task.FromResult<IQueueAdapter>(adapter);
        }

        public async Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return await StreamFailureHandlerFactory(queueId);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return _adapterCache;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return _streamQueueMapper;
        }

        #region helpers

        public static KafkaTopicAdapterFactory Create(IServiceProvider serviceProvider, string name)
        {
            var factory = ActivatorUtilities.CreateInstance<KafkaTopicAdapterFactory>(serviceProvider, name);
            factory.Init();
            return factory;
        }

        public virtual void Init()
        {
            StreamFailureHandlerFactory ??= ((qid) => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler()));
        }

        #endregion
    }
}