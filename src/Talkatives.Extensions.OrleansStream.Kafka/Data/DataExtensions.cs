using Confluent.Kafka;
using Orleans.Runtime;

namespace Talkatives.Extensions.OrleansStream.Kafka.Data
{
    public static class DataExtensions
    {
        public static long GetIdentifier<TKey, TValue>(this ConsumeResult<TKey, TValue> result)
        {
            return HashCode.Combine(result.Topic, result.Partition.Value, result.Offset.Value);
        }

        public static StreamId GetStreamId(this string id)
        {
            var parts = id.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length != 2)
            {
                return StreamId.Create("", "");
            }
            return StreamId.Create(parts[0], parts[1]);
        }
    }
}
