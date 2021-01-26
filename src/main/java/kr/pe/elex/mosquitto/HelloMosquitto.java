/*
Copyright (c) 2021. Elex. All Rights Reserved.
https://www.elex-project.com/
 */
package kr.pe.elex.mosquitto;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.cert.CertificateException;

/**
 * Mosquitto MQTT Example
 *
 * @author Elex
 */
@Slf4j
public class HelloMosquitto {
	static final String BROKER_URI = "tcp://127.0.0.1:1883";
	static final String BROKER_URI_TLS = "ssl://127.0.0.1:8883";

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
		connOpts.setUserName("elex");
		connOpts.setPassword("test".toCharArray());
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
		connOpts.setUserName("elex");
		connOpts.setPassword("test".toCharArray());
		connOpts.setAutomaticReconnect(true);
		connOpts.setCleanSession(true);
		connOpts.setSocketFactory(socketFactory);
		connOpts.setHttpsHostnameVerificationEnabled(false);

		client.connect(connOpts);
	}

	/**
	 * SSL Context를 만든 다음, 소켓 팩토리를 가져옵니다.
	 *
	 * @return
	 * @throws KeyStoreException
	 * @throws CertificateException
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws UnrecoverableKeyException
	 * @throws KeyManagementException
	 */
	static SSLSocketFactory socketFactory() throws KeyStoreException, CertificateException,
			NoSuchAlgorithmException, IOException, UnrecoverableKeyException, KeyManagementException {
		// OpenSSL로 키와 인증서를 만들고, PKCS12 키 저장소에 넣었습니다.
		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		// 클라이언트 키와 인증서가 들어있습니다.
		keyStore.load(HelloMosquitto.class.getResourceAsStream("/clientstore.p12"),
				"test".toCharArray()); // 키스토어 비번
		KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
		kmf.init(keyStore, "test1".toCharArray()); // 키 비번

		KeyStore trustKeyStore = KeyStore.getInstance("PKCS12");
		// CA 인증서가 들어있습니다.
		trustKeyStore.load(HelloMosquitto.class.getResourceAsStream("/truststore.p12"),
				"test".toCharArray()); // 키 스토어 비번
		TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
		tmf.init(trustKeyStore);

		SSLContext context = SSLContext.getInstance("TLSv1.3");
		context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

		return context.getSocketFactory();
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

		HelloMosquitto client1 = new HelloMosquitto("Client_1", socketFactory());
		client1.subscribe("hello/#", 1, new IMqttMessageListener() {
			@Override
			public void messageArrived(String topic, MqttMessage message) throws Exception {
				log.info("Rx: {} = {}", topic, new String(message.getPayload(), StandardCharsets.UTF_8));
			}
		});
		HelloMosquitto client2 = new HelloMosquitto("Client_2");
		client2.publish(String.join("/", "hello", "mosquitto"), "Hahaha, ...", 1);


		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			log.error("Interrupted ...", e);
		}
		client1.close();
		client2.close();
	}
}
