using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Talkatives.Extensions.OrleansStream.Kafka.Distributors;

namespace Talkatives.Extensions.OrleansStream.Kafka.Providers
{
    public class KafkaTopicDataManager
    {
        private readonly string _name;
        private readonly ILogger<KafkaTopicDataManager> _logger;
        private readonly KafkaMessageDistributor _messageDistributor;

        #region ctor

        public KafkaTopicDataManager(string name,
            IServiceProvider serviceProvider)
        {
            _name = name;
            _messageDistributor = serviceProvider.GetRequiredServiceByName<KafkaMessageDistributor>(name);
            _logger = serviceProvider.GetRequiredService<ILogger<KafkaTopicDataManager>>();
        }

        #endregion

        #region broker operations

        //todo: kafka work - will check if the topic needs to be created in Kafka.
        public async Task InitTopicAsync()
        {
            await Task.CompletedTask;
        }

        //todo: kafka work
        public async Task AddTopicMessage(string key, string message)
        {
            try
            {
                await _messageDistributor.Publish(new Message<string, string>
                                                        {
                                                            Headers = { },
                                                            Key = key,
                                                            Value = message
                                                        });
            }
            catch (Exception exc)
            {
                _logger.LogError(exc.ToString());
            }
        }

        #endregion
    }
}