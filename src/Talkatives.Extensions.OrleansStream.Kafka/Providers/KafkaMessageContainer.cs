using Orleans.Runtime;
using Orleans.Streams;
using Talkatives.Extensions.OrleansStreams.Abstractions;

namespace Talkatives.Extensions.OrleansStream.Kafka.Providers
{
    public class KafkaMessageContainer
    {
        #region .ctor

        public KafkaMessageContainer(StreamId streamId,
            StreamSequenceToken token,
            Dictionary<string, object> requestContext,
            IEnumerable<IMessageBody> events
            )
        {
            StreamId = streamId;
            Token = token;
            Header = requestContext;
            Events = events;
        }

        #endregion

        public StreamId StreamId { get; private set; }

        public StreamSequenceToken Token { get; private set; }

        public Dictionary<string, object> Header { get; private set; }

        public IEnumerable<IMessageBody> Events { get; set; }
    }
}