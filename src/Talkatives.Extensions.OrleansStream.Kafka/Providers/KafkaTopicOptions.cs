namespace Talkatives.Extensions.OrleansStream.Kafka.Providers
{
    public class KafkaTopicOptions
    {
        #region ctor

        public KafkaTopicOptions()
        {
            PartitionCount = 1;
        }

        #endregion

        public int PartitionCount { get; set; }

        public TimeSpan? MessageVisibilityTimeout { get; set; }

        #region helpers

        public IReadOnlyList<string> GetPartitionIds()
        {
            return Enumerable.Range(0, PartitionCount).Select(p => p.ToString()).ToList();
        }

        #endregion
    }

    public class KafkaTopicOptionsValidator : IConfigurationValidator
    {
        #region IConfigurationValidator

        public void ValidateConfiguration()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}