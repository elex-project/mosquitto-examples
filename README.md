# Mosquitto Example

```kotlin
implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5")
```

```java
MqttClient client = new MqttClient(BROKER_URI, clientId, persistence);
MqttConnectOptions connOpts = new MqttConnectOptions();
connOpts.setUserName("elex");
connOpts.setPassword("test".toCharArray());
connOpts.setAutomaticReconnect(true);
connOpts.setCleanSession(true);

client.connect(connOpts);
```

```java
MqttMessage msg = new MqttMessage(message.getBytes(StandardCharsets.UTF_8));
msg.setQos(qos);
client.publish(topic, msg);
```

```java
client.subscribe(topic, qos, listener);
```

------
Copyright (c) 2021. Elex.

All Rights Reserved.

https://www.elex-project.com/
