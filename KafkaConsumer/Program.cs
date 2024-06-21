using Confluent.Kafka;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KafkaConsumer
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "my-consumer-group",
                BootstrapServers = "127.0.0.1:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe("mqttMsg-topic");
            var cancellationToken = new CancellationTokenSource();
            Console.WriteLine("Press Enter Ctrl+C to exit.");
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cancellationToken.Cancel();
            };
            try
            {
                var client = new MongoClient("mongodb://127.0.0.1:27017");
                var collection = client.GetDatabase("mqtttest").GetCollection<BsonDocument>($"history_{DateTime.UtcNow.Year}_{DateTime.UtcNow.Month}");
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken.Token);
                        Console.WriteLine($"收到Kafka消息 '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");
                        var document = new BsonDocument
                        {
                            { "clientId", consumeResult.Message.Key },
                            { "JsonData", MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(consumeResult.Message.Value) },//不同设备上报数据格式不一定一样
                            { "created", DateTime.UtcNow }
                        };
                        await collection.InsertOneAsync(document);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"处理Kafka消息异常: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}
