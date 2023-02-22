using Orleans.Concurrency;
using Orleans.Streams;

namespace Sample.Talkatives.OrleansStream.Kafka.Grains
{
    public class KafkaSubActor : Grain, IKafkaSubscriber
    {
        private readonly ILogger<KafkaSubActor> _logger;
        private KafkaGrainNotificationHandler _notificationHandler;


        #region ctor

        public KafkaSubActor(ILogger<KafkaSubActor> logger)
        {
            _logger = logger;
        }

        #endregion

        #region IKafkaSubscriber

        public async Task Subscribe(string id)
        {
            _notificationHandler = new KafkaGrainNotificationHandler(
                                           id,
                                           this.GetStreamProvider(Constants.KafkaProviderName),
                                           OnNextAsync,
                                           OnCompleteAsync,
                                           OnErrorAsync,
                                           _logger);
            await _notificationHandler.Subscribe(default);
        }

        #endregion

        #region overrides

        public async override Task OnActivateAsync(CancellationToken token)
        {
            await base.OnActivateAsync(token);
        }

        #endregion

        #region notification

        [OneWay]
        private async Task OnNextAsync(KafkaND msg, StreamSequenceToken _ = null)
        {
            try
            {
                _logger.LogInformation($"Subscriber: {this.GetGrainId().ToString()} received event from stream: {_notificationHandler.GetStreamId()}");
            }
            catch (Exception e)
            {
                _logger.LogWarning($"Exception OnNextAsync: {e}");
            }
            await Task.CompletedTask;
        }

        [OneWay]
        private async Task OnCompleteAsync()
        {
            await Task.CompletedTask;
        }

        [OneWay]
        private async Task OnErrorAsync(Exception arg)
        {
            _logger.LogError("Received exception while subscribing: {0}.", arg);
            await Task.CompletedTask;
        }

        #endregion
    }

    public interface IKafkaSubscriber : IGrainWithStringKey
    {
        Task Subscribe(string id);
    }
}
