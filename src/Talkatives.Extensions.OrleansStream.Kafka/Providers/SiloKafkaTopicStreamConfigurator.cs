using Microsoft.Extensions.DependencyInjection;
using Talkatives.Extensions.OrleansStream.Kafka.Providers;

namespace Talkatives.Extensions.OrleansStream.Kafka.Providers
{
    public class SiloKafkaTopicStreamConfigurator : SiloPersistentStreamConfigurator, ISiloPersistentStreamConfigurator
    {
        #region .ctor

        public SiloKafkaTopicStreamConfigurator(string name,
            Action<Action<IServiceCollection>> configureServicesDelegate) : base(name, configureServicesDelegate, KafkaTopicAdapterFactory.Create)
        {
        }

        #endregion

        public string Dummy { get; set; }
    }
}
