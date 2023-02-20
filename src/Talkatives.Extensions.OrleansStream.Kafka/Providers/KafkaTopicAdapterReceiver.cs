using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;
using System.Threading.Tasks.Dataflow;
using Talkatives.Extensions.OrleansStream.Kafka.Data;
using Talkatives.Extensions.OrleansStream.Kafka.Distributors;

namespace Talkatives.Extensions.OrleansStream.Kafka.Providers
{
    public class KafkaTopicAdapterReceiver : IQueueAdapterReceiver
    {
        private long lastReadMessage;

        private KafkaTopicDataManager _dataManager;
        private readonly QueueId _queueId;
        private readonly ILogger<KafkaTopicAdapterReceiver> _logger;
        private readonly IQueueDataAdapter<string, IBatchContainer> _dataAdapter;

        private readonly BufferBlock<KMessage> _messageBuffer;
        private readonly KafkaMessageDistributor _messageDistributor;

        #region ctor

        public KafkaTopicAdapterReceiver(string name,
            QueueId queueId,
            KafkaTopicDataManager dataManager,
            IServiceProvider serviceProvider)
        {
            _queueId = queueId;
                       
            _dataManager = dataManager;
            _logger = serviceProvider.GetRequiredService<ILogger<KafkaTopicAdapterReceiver>>();
            _dataAdapter = serviceProvider.GetRequiredServiceByName<IQueueDataAdapter<string, IBatchContainer>>(name);
            _messageDistributor = serviceProvider.GetRequiredServiceByName<KafkaMessageDistributor>(name);

            _messageBuffer = new();
        }

        #endregion

        #region IQueueAdapterReceiver

        //todo: kafka - revisit this 
        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            List<IBatchContainer> msgs = new();
            try
            {
                var cToken = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
                var result = await _messageBuffer.ReceiveAsync(cToken.Token);
                if (result == null)
                {
                    return msgs;
                };
                KafkaTopicBatchContainer container = (KafkaTopicBatchContainer)_dataAdapter.FromQueueMessage(result.Data.Message.Value, lastReadMessage++);
                container.KafkaKey = result.DataKey;
                msgs.Add(container);
                return msgs;
            }
            catch(TaskCanceledException ex)
            {
                return msgs;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex.ToString());
                await Task.Delay(15000);
            }
            return msgs;
        }

        //todo: kafka - revisit this
        public async Task Initialize(TimeSpan timeout)
        {
            _messageDistributor.Subscribe(_queueId, _messageBuffer);
            await Task.CompletedTask;
        }

        //todo: kafka - revisit this
        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            if (messages == null)
            {
                return;
            }
            var kKeys = messages.Where(m => m is KafkaTopicBatchContainer)
                .Cast<KafkaTopicBatchContainer>()
                .Select(m => m.KafkaKey)
                .ToList();
            _messageDistributor.Commit(kKeys);
            await Task.CompletedTask;
        }

        //todo: kafka - revisit this
        public async Task Shutdown(TimeSpan timeout)
        {
            await Task.CompletedTask;
        }

        #endregion

        #region helpers

        public static KafkaTopicAdapterReceiver Create(string name, QueueId queueId, KafkaTopicDataManager dataManager, IServiceProvider serviceProvider)
        {
            return new KafkaTopicAdapterReceiver(name, queueId, dataManager, serviceProvider);
        }

        #endregion
    }
}