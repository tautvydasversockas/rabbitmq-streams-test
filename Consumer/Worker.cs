using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Net;
using System.Text;

namespace Consumer
{
    public sealed class Worker : BackgroundService
    {
        private ILoggerFactory? _loggerFactory;
        private StreamSystem? _streamSystem;
        private RabbitMQ.Stream.Client.Reliable.Consumer? _consumer;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _loggerFactory = CreateLoggerFactory();

            var streamSystemLogger = _loggerFactory.CreateLogger<StreamSystem>();
            _streamSystem = await CreateStreamSystemAsync(streamSystemLogger);

            var consumerLogger = _loggerFactory.CreateLogger<RabbitMQ.Stream.Client.Reliable.Consumer>();
            _consumer = await CreateConsumerAsync(_streamSystem, streamName: "messages", consumerLogger);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            if (_consumer is not null)
                await _consumer.Close();

            if (_streamSystem is not null)
                await _streamSystem.Close();

            _loggerFactory?.Dispose();

            await base.StopAsync(cancellationToken);
        }

        private static ILoggerFactory CreateLoggerFactory()
        {
            return LoggerFactory.Create(builder =>
            {
                builder.AddSimpleConsole();
                builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
            });
        }

        private static async Task<RabbitMQ.Stream.Client.Reliable.Consumer> CreateConsumerAsync(
            StreamSystem streamSystem, string streamName, ILogger<RabbitMQ.Stream.Client.Reliable.Consumer> logger)
        {
            var config = new ConsumerConfig(streamSystem, streamName)
            {
                IsSuperStream = true,
                Reference = "my-consumer",
                MessageHandler = async (sourceStream, consumer, messageContext, message) =>
                {
                    var myMessageAsText = Encoding.UTF8.GetString(message.Data.Contents);
                    logger.LogInformation("Consumed {message}", myMessageAsText);
                    await consumer.StoreOffset(messageContext.Offset);
                },
                ConsumerUpdateListener = async (reference, stream, isActive) => 
                {
                    ulong offset;
                    try
                    {
                        offset = await streamSystem.QueryOffset(reference, stream);
                    }
                    catch (OffsetNotFoundException)
                    {
                        return new OffsetTypeNext();
                    }

                    return new OffsetTypeOffset(offset + 1);
                },
            };
            return await RabbitMQ.Stream.Client.Reliable.Consumer.Create(config, logger);
        }

        private static Task<StreamSystem> CreateStreamSystemAsync(ILogger<StreamSystem> logger)
        {
            var config = new StreamSystemConfig
            {
                Endpoints = new List<EndPoint>
                {
                    new IPEndPoint(IPAddress.Loopback, 5552)
                }
            };
            return StreamSystem.Create(config, logger);
        }
    }
}
