using Orleans.Runtime;
using Orleans.Streams;

namespace Sample.Talkatives.OrleansStream.Kafka.Grains
{
    public class KafkaPubActor : Grain, IKafkaPublisher
    {
        private readonly ILogger _logger;
        private IAsyncStream<KafkaND> _stream;

        #region ctor

        public KafkaPubActor(ILogger<KafkaPubActor> logger)
        {
            _logger = logger;
        }

        #endregion

        #region IKafkaPublisher

        public async Task Publish()
        {
            try
            {
                await _stream.OnNextAsync(new KafkaND(_stream.StreamId.ToString(), "this is a test"));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
            }
        }

        #endregion

        #region overrides

        public async override Task OnActivateAsync(CancellationToken token)
        {
            var streamId = StreamId.Create(Constants.KafkaNameSpace, this.GetGrainId().Key.ToString());
            var streamProvider = this.GetStreamProvider(Constants.KafkaProviderName);
            _stream = streamProvider.GetStream<KafkaND>(streamId);
            await base.OnActivateAsync(token);
        }

        #endregion
    }

    public interface IKafkaPublisher : IGrainWithStringKey
    {
        Task Publish();
    }
}
