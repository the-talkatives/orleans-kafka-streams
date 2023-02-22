using Microsoft.AspNetCore.Mvc;
using Sample.Talkatives.OrleansStream.Kafka.Grains;

namespace Sample.Talkatives.OrleansStream.Kafka.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class KafkaStreamController : ControllerBase
    {
        private readonly IGrainFactory _grainFactory;

        #region ctor

        public KafkaStreamController(IGrainFactory grainFactory)
        {
            _grainFactory = grainFactory;
        }

        #endregion

        [HttpGet]
        [Route("publish/{id}")]
        public async Task<IActionResult> Publish(string id)
        {
            var grain = _grainFactory.GetGrain<IKafkaPublisher>(id);
            await grain.Publish();
            return new OkObjectResult(new
            {
                Success = true,
            });
        }

        [HttpGet]
        [Route("subscribe/{id}")]
        public async Task<IActionResult> Subscribe(string id)
        {
            var grain = _grainFactory.GetGrain<IKafkaSubscriber>(id);
            await grain.Subscribe(id);
            return new OkObjectResult(new
            {
                Success = true,
            });
        }

        [HttpGet]
        [Route("multi-sub/{id}/{subCount}")]
        public async Task<IActionResult> MultiSubscribe(string id, int subCount)
        {
            subCount = subCount <= 1 ? 1 : subCount;
            for (int i = 0; i < subCount; i++)
            {
                var grain = _grainFactory.GetGrain<IKafkaSubscriber>(Guid.NewGuid().ToString());
                await grain.Subscribe(id);
            }
            return new OkObjectResult(new
            {
                Success = true,
            });
        }
    }
}
