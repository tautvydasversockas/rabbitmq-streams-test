# RabbitMQ Streams testing playground

## Running

### 1. Run RabbitMQ

```
docker run -it -d --rm --name rabbitmq -p 5552:5552 -p 5672:5672 -p 15672:15672 -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" rabbitmq:3.11-management
```	

### 2. Enable streams plugin

```
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_stream
```

### 3. Restart RabbitMQ

```
docker restart rabbitmq
```

### 4. Create a super stream

```
docker exec rabbitmq rabbitmq-streams add_super_stream messages --partitions 3
```

### 5. Run producer & consumer applications

To run the producer, execute the following command from the repository root directory:
```
dotnet run --project ./Producer/Producer.csproj
```

To run the consumer, execute the following command from the repository root directory:
```
dotnet run --project ./Consumer/Consumer.csproj
```
