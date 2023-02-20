using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
using Talkatives.Extensions.OrleansStream.Kafka.Data;

namespace Talkatives.Extensions.OrleansStream.Kafka.Distributors
{
    public class KafkaMessageDistributor
    {
        private readonly string _topicName;
        private readonly IStreamQueueMapper _streamQueueMapper;
        private readonly ConsumerConfig _consumerConfig;
        private readonly ProducerConfig _producerConfig;
        private readonly IStreamQueueMapper _kafkaPartitionMapper;
        private DateTime? _seekTime;
        private readonly ILogger<KafkaMessageDistributor> _logger;

        private readonly ConcurrentDictionary<QueueId, HashSet<BufferBlock<KMessage>>> _consumers;
        private readonly BufferBlock<KMessage> _internalSubscriberQ;
        private readonly ConcurrentDictionary<KafkaKey, ConsumeResult<string, string>> _commitPoints;

        private IConsumer<string, string> _consumer;
        private IProducer<string, string> _producer;
        private readonly CancellationTokenSource _consumerCancellationToken;

        #region ctor

        public KafkaMessageDistributor(string topicName,
            ConsumerConfig consumerConfig,
            ProducerConfig producerConfig,
            IStreamQueueMapper kafkaPartitionMapper,
            DateTime? seekTime,
            ILogger<KafkaMessageDistributor> logger)
        {
            _topicName = topicName;
            _consumerConfig = consumerConfig;
            _producerConfig = producerConfig;
            _kafkaPartitionMapper = kafkaPartitionMapper;
            _seekTime = seekTime;
            _logger = logger;
            _consumers = new();
            _internalSubscriberQ = new(new DataflowBlockOptions
            {
                BoundedCapacity = 10
            });
            _commitPoints = new();

            _ = GetReaderStream();
            _ = GetWriterStream();

            StartSubscriberDistributor();
            _consumerCancellationToken = new CancellationTokenSource();
            StartSubscriberReader(_consumerCancellationToken);
        }

        #endregion

        #region helpers

        public async Task Publish(Message<string, string> msg)
        {
            try
            {
                await GetWriterStream().ProduceAsync(_topicName, msg);
            }
            catch (Exception exc)
            {
                _logger.LogError(exc.ToString());
            }
        }

        public void Subscribe(QueueId partitionId, BufferBlock<KMessage> queue)
        {
            var consumers = new HashSet<BufferBlock<KMessage>>();
            consumers.Add(queue);
            var result = _consumers.GetOrAdd(partitionId, consumers);
            if (!result.Contains(queue))
            {
                result.Add(queue);
            }
        }

        public void Commit(IEnumerable<KafkaKey> keys)
        {
            keys.ToList().ForEach(key =>
            {
                if (_commitPoints.TryRemove(key, out var result) && result != null)
                {
                    GetReaderStream().StoreOffset(result);
                }
            });
        }

        public void StopConsumer()
        {
            if (_consumerCancellationToken.Token.CanBeCanceled)
            {
                _consumerCancellationToken.Cancel();
            }
        }

        #endregion

        #region kafka subscription

        private IConsumer<string, string> GetReaderStream()
        {
            if (_consumer != null)
            {
                return _consumer;
            }
            var cBuilder = new ConsumerBuilder<string, string>(_consumerConfig);
            cBuilder.SetErrorHandler(HandleConsumerError);
            _consumer = cBuilder.Build();
            _consumer.Subscribe(_topicName);
            SeekByTimestamp();
            return _consumer;
        }

        private void HandleConsumerError(IConsumer<string, string> consumer, Error error)
        {
            var consumerId = $"{string.Join(",", consumer.Subscription)}::{_consumerConfig.GroupId}";
            _logger.LogWarning($"*****************Consumer: {consumerId} failed with Error Code: {error.Code}***************");
            if (error.IsFatal)
            {
                _logger.LogError($"Fatal error noticed in consumer: {consumerId}. Closing to reconnect.");
            }
        }

        private void SeekByTimestamp()
        {
            try
            {
                if (_seekTime == null ||
                    _seekTime.Value == DateTime.MinValue || _consumer?.Assignment == null)
                {
                    return;
                }
                var dtAsTimestamp = new Timestamp(_seekTime.Value.ToUniversalTime());

                //note: Assignments may not be available right away, will need to kind of nop for sometime before getting it.
                SpinWait.SpinUntil(() => GetReaderStream().Assignment?.Count > 0, 5000);
                var seekTopicPartitions = GetReaderStream().Assignment
                    .Select(tp => new TopicPartitionTimestamp(tp, dtAsTimestamp))
                    .ToList();
                if (seekTopicPartitions.Count == 0)
                {
                    return;
                }
                var partitionOffsets = GetReaderStream().OffsetsForTimes(seekTopicPartitions, TimeSpan.FromSeconds(30));
                if (partitionOffsets == null ||
                    partitionOffsets.Count == 0)
                {
                    return;
                }
                foreach (var item in partitionOffsets)
                {
                    GetReaderStream().Seek(item);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Seeking by Timestamp failed: {ex}");
            }
            finally
            {
                _seekTime = null;
            }
        }

        private void StartSubscriberReader(CancellationTokenSource cancellationToken)
        {
            Task.Run(async () =>
            {
                try
                {
                    GetReaderStream().Subscribe(_topicName);
                    SeekByTimestamp();
                    do
                    {
                        try
                        {
                            var result = GetReaderStream().Consume(1000);
                            if (result == null) continue;

                            var kKey = new KafkaKey(_topicName, result.Partition, result.Offset);
                            _commitPoints.TryAdd(kKey, result);
                            await _internalSubscriberQ.SendAsync(new KMessage(kKey, result));
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex.ToString());
                            await Task.Delay(15000);
                        }

                    } while (!cancellationToken.IsCancellationRequested);

                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.ToString());
                }
            });
        }

        #endregion

        #region kafka producer

        private IProducer<string, string> GetWriterStream()
        {
            //TODO: build producer builder
            if (_producer != null)
            {
                return _producer;
            }
            _producer = new ProducerBuilder<string, string>(_producerConfig)
                .Build();
            return _producer;
        }

        #endregion

        #region internal Q handler

        private void StartSubscriberDistributor()
        {
            Task.Run(async () =>
            {
                do
                {
                    try
                    {
                        var result = await _internalSubscriberQ.ReceiveAsync();
                        if (result == null)
                        {
                            continue;
                        }
                        var streamId = result.Data.Message.Key.GetStreamId();
                        var partitionId = _kafkaPartitionMapper.GetQueueForStream(streamId);
                        if (!_consumers.TryGetValue(partitionId, out var listeners) || listeners is null)
                        {
                            _logger.LogWarning($"No consumer found for Partition: {partitionId}. The stream id is: {streamId}");
                            _consumer.StoreOffset(result.Data);
                            continue;
                        }
                        foreach (var listener in listeners)                        
                        {
                            await listener.SendAsync(result);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex.ToString());
                        await Task.Delay(7000);
                    }

                } while (await _internalSubscriberQ.OutputAvailableAsync());
            });
        }

        #endregion
    }
}