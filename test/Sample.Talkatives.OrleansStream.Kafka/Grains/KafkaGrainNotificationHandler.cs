using Orleans.Runtime;
using Orleans.Streams;
using Talkatives.Extensions.OrleansStreams.Abstractions;

namespace Sample.Talkatives.OrleansStream.Kafka.Grains
{
    internal class KafkaGrainNotificationHandler
    {
        private StreamSubscriptionHandle<KafkaND> _subscriptionHandle;

        private readonly string _id;
        private readonly IStreamProvider _streamProvider;
        private readonly Func<KafkaND, StreamSequenceToken, Task> _onNextAsync;
        private readonly Func<Exception, Task> _onErrorAsync;
        private readonly Func<Task> _onCompleteAsync;
        private readonly ILogger _logger;

        #region ctor

        public KafkaGrainNotificationHandler(string id,
            IStreamProvider streamProvider,
            Func<KafkaND, StreamSequenceToken, Task> onNextAsync,
            Func<Task> onCompleteAsync,
            Func<Exception, Task> onErrorAsync,
            ILogger logger)
        {
            _id = id;
            _streamProvider = streamProvider;
            _logger = logger;
            _onNextAsync = onNextAsync;
            _onCompleteAsync = onCompleteAsync;
            _onErrorAsync = onErrorAsync;
        }

        #endregion

        public string GetStreamId()
        {
            return _streamProvider.Name;
        }

        #region notification

        public async Task Subscribe(object arg)
        {
            try
            {
                if (_subscriptionHandle != null)
                {
                    _logger.LogWarning($"Subscribe exiting for grain {nameof(KafkaGrainNotificationHandler)} with Key: {_id}. Successfully Registered?: {_subscriptionHandle != null}");
                    await _subscriptionHandle.UnsubscribeAsync();
                }
                await SetSubscription();
            }
            catch
            {
            }
        }

        private async Task SetSubscription()
        {
            var streamId = StreamId.Create(Constants.KafkaNameSpace, _id);
            _subscriptionHandle = await SetSubscriptionStreamAsync(streamId, _streamProvider, _onNextAsync, _onErrorAsync, _onCompleteAsync);
        }

        private async Task<StreamSubscriptionHandle<KafkaND>> SetSubscriptionStreamAsync(StreamId streamId,
                                                                                    IStreamProvider streamProvider,
                                                                                    Func<KafkaND, StreamSequenceToken, Task> onNextAsync,
                                                                                    Func<Exception, Task> onErrorAsync,
                                                                                    Func<Task> onCompleteAsync)
        {
            StreamSubscriptionHandle<KafkaND> handle = null;
            try
            {
                var stream = streamProvider.GetStream<KafkaND>(streamId);
                var subscriptionHandles = await stream.GetAllSubscriptionHandles();

                if (subscriptionHandles == null || subscriptionHandles.Count <= 0)
                {
                    return await stream.SubscribeAsync(onNextAsync, onErrorAsync, onCompleteAsync);
                }

                var first = true;
                foreach (var subHandle in subscriptionHandles)
                {
                    if (first)
                    {
                        handle = await subHandle.ResumeAsync(onNextAsync);
                        first = false;
                        continue;
                    }
                    await subHandle.UnsubscribeAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error in SetSubscriptionStreamAsync for grain :{_id} {ex}");
            }
            return handle;
        }

        public async Task UnSubscribe()
        {
            _logger.LogInformation($"Unsubscribe called on grain id: {_id}.");
            if (_subscriptionHandle == null)
            {
                return;
            }
            await _subscriptionHandle.UnsubscribeAsync();
            _subscriptionHandle = null;
        }

        #endregion
    }

    [GenerateSerializer]
    public class KafkaND : IMessageBody
    {
        #region ctor

        public KafkaND(string key,
            string data)
        {
            Key = key;
            Data = data;
        }

        #endregion

        [Id(0)]
        public string Key { get; private set; }

        [Id(1)]
        public string Data { get; private set; }

        [Id(2)]
        public long Id { get; set; }
    }
}
