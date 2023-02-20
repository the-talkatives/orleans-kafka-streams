using Confluent.Kafka;

namespace Talkatives.Extensions.OrleansStream.Kafka.Data
{
    public class KMessage
    {
        #region ctor

        public KMessage(KafkaKey dataKey, ConsumeResult<string, string> data)
        {
            DataKey = dataKey;
            Data = data;
        }

        #endregion

        public KafkaKey DataKey { get; private set; }

        public ConsumeResult<string, string> Data { get; private set; }
    }

    [GenerateSerializer]
    public struct KafkaKey
    {
        #region ctor

        public KafkaKey(string topic, Partition pId, Offset offset)
        {
            Topic = topic;
            PartitionId = pId.Value;
            Offset = offset.Value;
        }

        #endregion

        [Id(0)]
        public string Topic { get; private set; }

        [Id(1)]
        public int PartitionId { get; private set; }

        [Id(2)]
        public long Offset { get; private set; }

        #region overrides

        public override int GetHashCode()
        {
            return HashCode.Combine(Topic, PartitionId, Offset);
        }

        public override string ToString()
        {
            return $"{Topic}:{PartitionId}:{Offset}";
        }

        #endregion
    }
}
