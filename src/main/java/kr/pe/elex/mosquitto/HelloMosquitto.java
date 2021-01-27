/*
Copyright (c) 2021. Elex. All Rights Reserved.
https://www.elex-project.com/
 */
package kr.pe.elex.mosquitto;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

/**
 * Mosquitto MQTT Example
 *
 * @author Elex
 */
@Slf4j
public final class HelloMosquitto {
	static final String BROKER_URI = "tcp://127.0.0.1:1883";
	static final String BROKER_URI_TLS = "ssl://127.0.0.1:8883";
	static final String USER_NAME = "elex";
	static final String PASSWORD = "test";

	private MqttClient client;
	private MemoryPersistence persistence = new MemoryPersistence();

	/**
	 * MQTT 연결
	 *
	 * @param clientId 연결하는 클라이언트 마다 다르게 지정.
	 * @throws MqttException
	 */
	public HelloMosquitto(String clientId) throws MqttException {
		client = new MqttClient(BROKER_URI, clientId, persistence);
		MqttConnectOptions connOpts = new MqttConnectOptions();
		connOpts.setUserName(USER_NAME);
		connOpts.setPassword(PASSWORD.toCharArray());
		connOpts.setAutomaticReconnect(true);
		connOpts.setCleanSession(true);

		client.connect(connOpts);
	}

	/**
	 * MQTT over TLS 연결
	 *
	 * @param clientId      연결하는 클라이언트 마다 다르게 지정.
	 * @param socketFactory tls socket factory
	 * @throws MqttException
	 */
	public HelloMosquitto(String clientId, SSLSocketFactory socketFactory) throws MqttException {
		client = new MqttClient(BROKER_URI_TLS, clientId, persistence); // 주소는 ssl로 시작하고, 포트는 8883을 사용합니다.
		MqttConnectOptions connOpts = new MqttConnectOptions();
		connOpts.setUserName(USER_NAME);
		connOpts.setPassword(PASSWORD.toCharArray());
		connOpts.setAutomaticReconnect(true);
		connOpts.setCleanSession(true);
		connOpts.setSocketFactory(socketFactory);
		connOpts.setHttpsHostnameVerificationEnabled(false);

		client.connect(connOpts);
	}


	/**
	 * 발행
	 *
	 * @param topic   토픽. Word separator: '/'
	 * @param message 메시지
	 * @param qos     최대 전송 메시지 개수
	 * @throws MqttException
	 */
	public void publish(String topic, String message, int qos) throws MqttException {
		if (client.isConnected()) {
			MqttMessage msg = new MqttMessage(message.getBytes(StandardCharsets.UTF_8));
			msg.setQos(qos);

			client.publish(topic, msg);
			log.info("Tx: {} = {}", topic, new String(msg.getPayload(), StandardCharsets.UTF_8));
		}
	}

	/**
	 * 구독 시작
	 *
	 * @param topic    토픽 패턴. Wildcard char : '?' for 1 word, '#' for words
	 * @param qos      최대 전송 메시지 개수
	 * @param listener 리스너
	 * @throws MqttException
	 */
	public void subscribe(String topic, int qos, IMqttMessageListener listener) throws MqttException {
		if (client.isConnected()) {
			client.subscribe(topic, qos, listener);
		}
	}

	/**
	 * 구독 중단
	 *
	 * @param topic 토픽 패턴
	 * @throws MqttException
	 */
	public void unsubscribe(String topic) throws MqttException {
		if (client.isConnected()) {
			client.unsubscribe(topic);
		}
	}

	public void close() throws MqttException {
		client.disconnect();
	}

	public static void main(String... args)
			throws MqttException, CertificateException, UnrecoverableKeyException, NoSuchAlgorithmException,
			KeyStoreException, KeyManagementException, IOException {
		// TCP connection
		HelloMosquitto client1 = new HelloMosquitto("Client_1");
		client1.subscribe("hello/#", 1, new IMqttMessageListener() {
			@Override
			public void messageArrived(String topic, MqttMessage message) throws Exception {
				log.info("Rx: {} = {}", topic, new String(message.getPayload(), StandardCharsets.UTF_8));
			}
		});
		client1.publish(String.join("/", "hello", "tcp"), "Hi!", 1);

		// TLS connection
		HelloMosquitto client2 = new HelloMosquitto("Client_2", TlsHelper.socketFactory(
				HelloMosquitto.class.getResourceAsStream("/clientstore.p12"),
				"test".toCharArray(),
				"test1".toCharArray(),
				HelloMosquitto.class.getResourceAsStream("/truststore.p12"),
				"test".toCharArray()
		));
		client2.publish(String.join("/", "hello", "tls"), "Hahaha, ...", 1);

		// TLS with BC connection
		HelloMosquitto client3 = new HelloMosquitto("Client_3", TlsHelperWithBouncyCastle.socketFactory(
				HelloMosquitto.class.getResourceAsStream("/client.pem"),
				HelloMosquitto.class.getResourceAsStream("/client.key.pem"),
				"test1".toCharArray(),
				HelloMosquitto.class.getResourceAsStream("/ca.pem")
		));
		client3.publish(String.join("/", "hello", "tls", "bc"), "Oke, ...", 1);

		// TLS with BC connection, without client cert.
		HelloMosquitto client4 = new HelloMosquitto("Client_4", TlsHelperWithBouncyCastle.socketFactory(
				HelloMosquitto.class.getResourceAsStream("/ca.pem")
		));
		client4.publish(String.join("/", "hello", "tls", "bc-ca-only"), "Oke, ...", 1);

		// // TLS connection, without client key store.
		HelloMosquitto client5 = new HelloMosquitto("Client_4", TlsHelper.socketFactory(
				HelloMosquitto.class.getResourceAsStream("/truststore.p12"),
				"test".toCharArray())
		);
		client5.publish(String.join("/", "hello", "tls", "ca-only"), "Mmmm, ...", 1);

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			log.error("Interrupted ...", e);
		}
		client1.close();
		client2.close();
		client3.close();
		client4.close();
		client5.close();
	}
}
