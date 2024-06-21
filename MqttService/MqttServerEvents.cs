using Confluent.Kafka;
using MQTTnet.Internal;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using MQTTnet.Server;
using Newtonsoft.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MqttService
{
    /// <summary>
    /// mqtt服务事件
    /// </summary>
    public class MqttServerEvents
    {
        /// <summary>
        /// 服务器已启动
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_StartedAsync(EventArgs e)
        {
            Console.WriteLine($"MqttServer is  started");

            return Task.CompletedTask;
        }

        /// <summary>
        /// 服务器已停止
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_StoppedAsync(EventArgs e)
        {
            Console.WriteLine($"Server is stopped");
            return Task.CompletedTask;
        }

        /// <summary>
        /// 客户端已连接
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_ClientConnectedAsync(ClientConnectedEventArgs e)
        {
            Console.WriteLine($"Client [{e.ClientId}] {e.Endpoint} {e.UserName} connected");
            return Task.CompletedTask;
        }

        /// <summary>
        /// 客户端已断开, 用于设备主动断开和莫名断开
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_ClientDisconnectedAsync(ClientDisconnectedEventArgs e)
        {
            Console.WriteLine($"Client [{e.ClientId}] {e.Endpoint} disconnected");
            return Task.CompletedTask;
        }

        /// <summary>
        /// 客户端已订阅主题
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_ClientSubscribedTopicAsync(ClientSubscribedTopicEventArgs e)
        {
            Console.WriteLine($"Client [{e.ClientId}] subscribed [{e.TopicFilter}]");
            return Task.CompletedTask;
        }

        /// <summary>
        /// 客户端已退订主题
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_ClientUnsubscribedTopicAsync(ClientUnsubscribedTopicEventArgs e)
        {
            Console.WriteLine($"Client [{e.ClientId}] unsubscribed[{e.TopicFilter}]");
            return Task.CompletedTask;
        }

        /// <summary>
        /// 客户端连接检验器
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public async Task Server_ValidatingConnectionAsync(ValidatingConnectionEventArgs e)
        {
            var result = await ValidateConnectionAsync(e);
            if (result)
            {
                e.ReasonCode = MqttConnectReasonCode.Success;
                e.SessionItems.Add(nameof(e.ClientId), e.ClientId);//保存到session
            }
        }
        /// <summary>
        /// 连接检验
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        private async Task<bool> ValidateConnectionAsync(ValidatingConnectionEventArgs e)
        {
           return true;
        }


        /// <summary>
        /// 拦截MqttServer接收到的数据包。Mqtt客户端发出的消息都会被该方法拦截。
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public async Task Server_InterceptingInboundPacketAsync(InterceptingPacketEventArgs e)
        {
            if (e.ClientId !=null && e.Packet !=null && e.Packet.ToString().ToLower().Contains("ping"))
            {
                //ping消息处理
                return;
            }
            Console.WriteLine($"Client [{e.ClientId}] InterceptingInboundPacket[{e.Packet}]");
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
            using var producer = new ProducerBuilder<string, string>(config).Build();
            try
            {
                var message = new Message<string, string>
                {
                    Key = e.ClientId,
                    Value = JsonConvert.SerializeObject(e.Packet)
                };
                var deliveryResult = await producer.ProduceAsync("mqttMsg-topic", message);
                Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
            }
            catch (ProduceException<string, string> ke)
            {
                Console.WriteLine($"Delivery failed: {ke.Error.Reason}");
            }
        }

        /// <summary>
        /// 拦截从MqttServer发送的数据包。Mqtt服务端成功地投递到目标客户端时都会被该方法拦截。
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_InterceptingOutboundPacketAsync(InterceptingPacketEventArgs e)
        {
            Console.WriteLine($"Client [{e.ClientId}] InterceptingOutboundPacket[{e.Packet}]");
            return Task.CompletedTask;
        }

        /// <summary>
        /// 拦截发布。Mqtt客户端发出的消息，以及服务端通过 InjectApplicationMessage 方法发出的消息都会被该方法拦截。
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_InterceptingPublishAsync(InterceptingPublishEventArgs e)
        {
            Console.WriteLine($"Sent topic='{e.ApplicationMessage.Topic}', content='{Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment)}'");
            return Task.CompletedTask;
        }

        /// <summary>
        /// 拦截已被客户端确认的数据包
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_ClientAcknowledgedPublishPacketAsync(ClientAcknowledgedPublishPacketEventArgs e)
        {
            return CompletedTask.Instance;
        }

        /// <summary>
        /// 拦截未被消费的消息
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public async Task Server_ApplicationMessageNotConsumedAsync(ApplicationMessageNotConsumedEventArgs e)
        {
            await Task.CompletedTask;
        }

        /// <summary>
        /// 保留消息已变更
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_RetainedMessageChangedAsync(RetainedMessageChangedEventArgs e)
        {
            try
            {
                 //File.WriteAllText(RetainedMessagesStorePath, JsonConvert.SerializeObject(e.StoredRetainedMessages)); 持久化保留消息
                 Console.WriteLine("Retained messages saved.");
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception.Message, "Error occured");
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// 加载保留消息
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_LoadingRetainedMessageAsync(LoadingRetainedMessagesEventArgs e)
        {
            try
            {
                
                //List<MqttApplicationMessage> retainedMessages;
                //if (File.Exists(RetainedMessagesStorePath))
                //{
                //    var json = File.ReadAllText(RetainedMessagesStorePath);
                //    retainedMessages = JsonConvert.DeserializeObject<List<MqttApplicationMessage>>(json);
                //}
                //else
                //{
                //    retainedMessages = new List<MqttApplicationMessage>();
                //}
                // e.LoadedRetainedMessages = retainedMessages;
                 Console.WriteLine("Retained messages loaded.");
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("No retained messages stored yet.");
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception.Message, "Error occured");
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// 删除保留消息
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public Task Server_RetainedMessagesClearedAsync(EventArgs e)
        {
            //File.Delete(RetainedMessagesStorePath);
            Console.WriteLine("Retained messages delete.");
            return Task.CompletedTask;
        }
  
    }

}
