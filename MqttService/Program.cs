using MQTTnet.Server;
using MQTTnet;

namespace MqttService
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var mqttFactory = new MqttFactory();
            var mqttServerOptions = new MqttServerOptionsBuilder()
                .WithDefaultEndpointPort(1883)//监听的端口
                .WithDefaultEndpoint()
                .WithoutEncryptedEndpoint()// 不启用tls
                .WithDefaultCommunicationTimeout(TimeSpan.FromSeconds(10 * 1000))//10秒超时
                .WithPersistentSessions(true)//启用session
                .WithConnectionBacklog(1000)//积累的最大连接请求数
                .Build();
            using (var mqttServer = mqttFactory.CreateMqttServer(mqttServerOptions))
            {
                AddMqttEvents(mqttServer);

                await mqttServer.StartAsync();
                Console.WriteLine("Press Enter Ctrl+C to exit.");
                Console.ReadLine();
                Console.CancelKeyPress += async (sender, e) =>
                {
                    e.Cancel = true; // 防止进程直接终止
                    await mqttServer.StopAsync();
                    Environment.Exit(0);
                };
            }
        }

        private static void AddMqttEvents(MqttServer mqttServer)
        {
            MqttServerEvents mqttEvents = new MqttServerEvents();
            mqttServer.ClientConnectedAsync += mqttEvents.Server_ClientConnectedAsync;
            mqttServer.StartedAsync += mqttEvents.Server_StartedAsync;
            mqttServer.StoppedAsync += mqttEvents.Server_StoppedAsync;
            mqttServer.ClientSubscribedTopicAsync += mqttEvents.Server_ClientSubscribedTopicAsync;
            mqttServer.ClientUnsubscribedTopicAsync += mqttEvents.Server_ClientUnsubscribedTopicAsync;
            mqttServer.ValidatingConnectionAsync += mqttEvents.Server_ValidatingConnectionAsync;
            mqttServer.ClientDisconnectedAsync += mqttEvents.Server_ClientDisconnectedAsync;
            mqttServer.InterceptingInboundPacketAsync += mqttEvents.Server_InterceptingInboundPacketAsync;
            mqttServer.InterceptingOutboundPacketAsync += mqttEvents.Server_InterceptingOutboundPacketAsync;
            mqttServer.InterceptingPublishAsync += mqttEvents.Server_InterceptingPublishAsync;
            mqttServer.ApplicationMessageNotConsumedAsync += mqttEvents.Server_ApplicationMessageNotConsumedAsync;
            mqttServer.ClientAcknowledgedPublishPacketAsync += mqttEvents.Server_ClientAcknowledgedPublishPacketAsync;
        }
    }
}
