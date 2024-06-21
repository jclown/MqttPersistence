## 概述

通过MQTT协议传递到Kafka，并由Kafka消费者处理后存储到MongoDB的简单示例

## 目录结构

KafkaConsumer：处理从Kafka消费数据并存储到MongoDB的逻辑。<br>
MqttClient：实现接收设备上传数据的MQTT客户端。<br>
MqttService：整合MQTT和Kafka的服务逻辑，实现数据流转发。<br>
docker-compose.yml：Docker配置文件，用于快速部署Kafka和MongoDB服务。<br>