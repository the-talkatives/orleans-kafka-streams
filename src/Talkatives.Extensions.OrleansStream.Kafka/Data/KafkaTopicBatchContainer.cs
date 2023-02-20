using Newtonsoft.Json;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using Talkatives.Extensions.OrleansStreams.Abstractions;

namespace Talkatives.Extensions.OrleansStream.Kafka.Data
{
    [GenerateSerializer]
    public class KafkaTopicBatchContainer : IBatchContainer
    {
        #region ctor

        [JsonConstructor]
        public KafkaTopicBatchContainer(
                StreamId streamId,
                List<IMessageBody> events,
                Dictionary<string, object> requestContext,
                EventSequenceTokenV2 sequenceToken)
            : this(streamId, events, requestContext)
        {
            StreamId = streamId;
            _events = events;
            _sequenceToken = sequenceToken;
            _requestContext = requestContext;
        }

        public KafkaTopicBatchContainer(StreamId streamId, List<IMessageBody> events, Dictionary<string, object> requestContext)
        {
            if (events == null) throw new ArgumentNullException(nameof(events), "Message contains no events");

            StreamId = streamId;
            _events = events;
            _requestContext = requestContext;
        }

        #endregion

        [JsonProperty]
        [Id(0)]
        private EventSequenceTokenV2 _sequenceToken;
        internal EventSequenceTokenV2 RealSequenceToken
        {
            set { _sequenceToken = value; }
        }

        [JsonProperty]
        [Id(1)]
        private readonly List<IMessageBody> _events;

        [JsonProperty]
        [Id(2)]
        private readonly Dictionary<string, object> _requestContext;

        #region IBatchContainer

        [Id(3)]
        public StreamId StreamId { get; }

        public StreamSequenceToken SequenceToken => _sequenceToken;

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return _events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, _sequenceToken.CreateSequenceTokenForEvent(i)));
        }

        public bool ImportRequestContext()
        {
            if (_requestContext != null)
            {
                RequestContextExtensions.Import(_requestContext);
                return true;
            }
            return false;
        }

        #endregion

        [Id(4)]
        public KafkaKey KafkaKey { get; set; }

        #region overrides

        public override string ToString()
        {
            return $"[KafkaTopicBatchContainer:Stream={StreamId},#Items={_events.Count}]";
        }

        #endregion
    }
}