using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;
using System.Net;
using System.Text;
using System.Text.Json;

namespace Producer
{
    public sealed class MyMessage
    {
        public required string Text { get; init; }
    };

    public sealed class Worker : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var loggerFactory = CreateLoggerFactory();

            var streamSystemLogger = loggerFactory.CreateLogger<StreamSystem>();
            var streamSystem = await CreateStreamSystemAsync(streamSystemLogger);

            var producerLogger = loggerFactory.CreateLogger<RabbitMQ.Stream.Client.Reliable.Producer>();
            var producer = await CreateProducerAsync(streamSystem, streamName: "messages", producerLogger);

            var rnd = new Random();
            var partitionKeys = new[] { "k1", "k2", "k3", "k4", "k5", "k6" };

            while (!stoppingToken.IsCancellationRequested)
            {
                var partitionKey = partitionKeys[rnd.Next(partitionKeys.Length)];
                var myMessage = new MyMessage
                {
                    Text = Guid.NewGuid().ToString("N")
                };
                var myMessageAsText = JsonSerializer.Serialize(myMessage);
                var messageData = Encoding.UTF8.GetBytes(myMessageAsText);
                var message = new Message(messageData)
                {
                    ApplicationProperties = new ApplicationProperties
                    {
                        ["MessageType"] = "MyMessage",
                        ["MessageVersion"] = 1,
                        ["PartitionKey"] = partitionKey
                    },
                    Properties = new Properties
                    {
                        MessageId = Guid.NewGuid().ToString("N"),
                        CorrelationId = Guid.NewGuid().ToString("N"),
                        ContentType = "application/json",
                        ContentEncoding = "utf-8",
                    }
                };
                await producer.Send(message);
                await Task.Delay(1_000, stoppingToken);
            }

            await producer.Close();
            await streamSystem.Close();
        }

        private static ILoggerFactory CreateLoggerFactory()
        {
            return LoggerFactory.Create(builder =>
            {
                builder.AddSimpleConsole();
                builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
            });
        }

        private static Task<RabbitMQ.Stream.Client.Reliable.Producer> CreateProducerAsync(
            StreamSystem streamSystem, string streamName, ILogger<RabbitMQ.Stream.Client.Reliable.Producer> logger)
        {
            var config = new ProducerConfig(streamSystem, streamName)
            {
                SuperStreamConfig = new SuperStreamConfig
                {
                    Routing = msg => msg.ApplicationProperties["PartitionKey"].ToString()
                },
                ConfirmationHandler = confirmation =>
                {
                    switch (confirmation.Status)
                    {
                        case ConfirmationStatus.Confirmed:
                            var message = confirmation.Messages.First();
                            var myMessageAsText = Encoding.UTF8.GetString(message.Data.Contents);
                            logger.LogInformation("Confirmed {message}", myMessageAsText);
                            break;

                        default:
                            logger.LogError("Message not confirmed with error: {status}", confirmation.Status);
                            break;
                    }

                    return Task.CompletedTask;
                }
            };
            return RabbitMQ.Stream.Client.Reliable.Producer.Create(config, logger);
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
