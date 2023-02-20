using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;
using System.Collections.Concurrent;
using Talkatives.Extensions.OrleansStreams.Abstractions;

namespace Talkatives.Extensions.OrleansStream.Kafka.Providers
{
    public class KafkaTopicAdapter : IQueueAdapter
    {
        private readonly ILogger<KafkaTopicAdapter> _logger;
        private readonly HashRingBasedPartitionedStreamQueueMapper _streamQueueMapper;
        private readonly IQueueDataAdapter<string, IBatchContainer> _dataAdapter;
        private readonly IServiceProvider _serviceProvider;

        private readonly ConcurrentDictionary<QueueId, KafkaTopicDataManager> _dataManagers = new();

        #region .ctor

        public KafkaTopicAdapter(string name,
            IServiceProvider serviceProvider)
        {
            Name = name;
            _serviceProvider = serviceProvider;

            _streamQueueMapper = (HashRingBasedPartitionedStreamQueueMapper)serviceProvider.GetRequiredServiceByName<IStreamQueueMapper>(Name);
            _dataAdapter = serviceProvider.GetRequiredServiceByName<IQueueDataAdapter<string, IBatchContainer>>(name);
            _logger = serviceProvider.GetRequiredService<ILogger<KafkaTopicAdapter>>();
        }

        #endregion

        #region IQueueAdapter

        public string Name { get; private set; }

        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        //todo: kafka changes figure out how to handle this.
        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            if (!_dataManagers.TryGetValue(queueId, out var kdm))
            {
                var partionId = _streamQueueMapper.QueueToPartition(queueId);
                kdm = new KafkaTopicDataManager(Name, _serviceProvider);
                _ = _dataManagers.TryAdd(queueId, kdm);
            }
            return KafkaTopicAdapterReceiver.Create(Name, queueId, kdm, _serviceProvider);
        }

        public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            if (token != null) throw new ArgumentException("KafkaTopic stream provider currently does not support non-null StreamSequenceToken.", nameof(token));
            var key = _streamQueueMapper.GetQueueForStream(streamId);
            if (!_dataManagers.TryGetValue(key, out var kdm))
            {
                kdm = new KafkaTopicDataManager(Name, _serviceProvider);
                await kdm.InitTopicAsync();
                kdm = _dataManagers.GetOrAdd(key, kdm);
            }
            foreach (var @event in events)
            {
                if (@event is IMessageBody evt)
                {
                    var cloudMsg = _dataAdapter.ToQueueMessage(streamId, new List<T>() { @event }, null, requestContext);
                    await kdm.AddTopicMessage(evt.Key, cloudMsg);
                }
            }
        }

        #endregion
    }
}
