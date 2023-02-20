using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using Talkatives.Extensions.OrleansStream.Kafka.Data;
using Talkatives.Extensions.OrleansStreams.Abstractions;

namespace Talkatives.Extensions.OrleansStream.Kafka.Providers
{
    public class KafkaTopicDataAdapter : IQueueDataAdapter<string, IBatchContainer>, IOnDeserialized
    {
        private Serializer<KafkaTopicBatchContainer> _serializer;

        #region ctor

        public KafkaTopicDataAdapter(Serializer serializer)
        {
            _serializer = serializer.GetSerializer<KafkaTopicBatchContainer>();
        }

        #endregion

        #region IQueueDataAdapter

        public IBatchContainer FromQueueMessage(string queueMessage, long sequenceId)
        {
            var kb = _serializer.Deserialize(Convert.FromBase64String(queueMessage));
            kb.RealSequenceToken = new EventSequenceTokenV2(sequenceId);
            return kb;
        }

        public string ToQueueMessage<TEvent>(StreamId streamId, IEnumerable<TEvent> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var bMessage = new KafkaTopicBatchContainer(streamId, events.Cast<IMessageBody>().ToList(), requestContext);
            var rawBytes = _serializer.SerializeToArray(bMessage);
            return Convert.ToBase64String(rawBytes);
        }

        #endregion

        #region IOnDeserialized

        public void OnDeserialized(DeserializationContext context)
        {
            _serializer = context.ServiceProvider.GetRequiredService<Serializer<KafkaTopicBatchContainer>>();
        }

        #endregion
    }
}
