using MQTTnet.Client;
using MQTTnet;
using System.Threading;

namespace MqttClient
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var mqttFactory = new MqttFactory();
            var mqttClient = mqttFactory.CreateMqttClient();

            var mqttOptions = new MqttClientOptionsBuilder()
                .WithClientId("MqttServiceClient")
                .WithTcpServer("127.0.0.1", 1883)
                .Build();
            mqttClient.ConnectedAsync+=(e =>
            {
                Console.WriteLine("MQTT连接成功");
                return Task.CompletedTask;
            });

            mqttClient.DisconnectedAsync+=(e =>
            {
                Console.WriteLine("MQTT连接断开");
                return Task.CompletedTask;
            });
            await mqttClient.ConnectAsync(mqttOptions, CancellationToken.None);
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                Environment.Exit(0);
            };
            Console.WriteLine("请输入消息.");
            var input="";
            while ((input = Console.ReadLine()) != "q")
            {
                if (!string.IsNullOrEmpty(input))
                {
                    MqttApplicationMessage applicationMessage = new MqttApplicationMessage
                    {
                        Topic = "mqtttest",
                        PayloadSegment = new ArraySegment<byte>(System.Text.Encoding.UTF8.GetBytes(input))
                    };

                    var res = await mqttClient.PublishAsync(applicationMessage);

                    if (res.ReasonCode == MqttClientPublishReasonCode.Success)
                    {
                        Console.WriteLine("发送成功.");
                    }
                    else
                    {
                        Console.WriteLine($"发送失败: {res.ReasonCode}");
                    }
                }
                else
                {
                    Console.WriteLine("请输入有效消息. 或输入 'q' 退出.");
                }
            }

            await mqttClient.DisconnectAsync();
        }
    }
}
