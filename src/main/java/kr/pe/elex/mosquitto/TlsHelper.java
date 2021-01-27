/*
Copyright (c) 2021. Elex. All Rights Reserved.
https://www.elex-project.com/
 */
package kr.pe.elex.mosquitto;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;

/**
 * @author Elex
 */
public final class TlsHelper {
	private TlsHelper() {
	}

	/**
	 * SSL Context
	 *
	 * @param keyStoreInputStream   키스토어
	 * @param keyStorePassword      키스토어 비번
	 * @param keyPassword           키 비번
	 * @param trustStoreInputStream CA 인증서 키 스토어
	 * @param trustStorePassword    트러스트 스토어 비번
	 * @return
	 * @throws KeyStoreException
	 * @throws CertificateException
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws UnrecoverableKeyException
	 * @throws KeyManagementException
	 */
	public static SSLContext context(InputStream keyStoreInputStream, char[] keyStorePassword, char[] keyPassword,
	                                 InputStream trustStoreInputStream, char[] trustStorePassword)
			throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException,
			KeyManagementException {
		// OpenSSL로 키와 인증서를 만들고, PKCS12 키 저장소에 넣었습니다.
		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		// 클라이언트 키와 인증서가 들어있습니다.
		keyStore.load(keyStoreInputStream,
				keyStorePassword); // 키스토어 비번
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(keyStore, keyPassword); // 키 비번

		KeyStore trustKeyStore = KeyStore.getInstance("PKCS12");
		// CA 인증서가 들어있습니다.
		trustKeyStore.load(trustStoreInputStream,
				trustStorePassword); // 키 스토어 비번
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(trustKeyStore);

		SSLContext context = SSLContext.getInstance("TLSv1.3");
		context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

		return context;
	}

	public static SSLSocketFactory socketFactory(InputStream keyStoreInputStream, char[] keyStorePassword, char[] keyPassword,
	                                             InputStream trustStoreInputStream, char[] trustStorePassword)
			throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException,
			KeyManagementException {
		return context(keyStoreInputStream, keyStorePassword, keyPassword, trustStoreInputStream, trustStorePassword)
				.getSocketFactory();
	}

	/**
	 * @param trustStoreInputStream CA 인증서 키 스토어
	 * @param trustStorePassword    트러스트 스토어 비번
	 * @return
	 * @throws KeyStoreException
	 * @throws CertificateException
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws UnrecoverableKeyException
	 * @throws KeyManagementException
	 */
	public static SSLContext context(InputStream trustStoreInputStream, char[] trustStorePassword)
			throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException,
			KeyManagementException {
		// OpenSSL로 키와 인증서를 만들고, PKCS12 키 저장소에 넣었습니다.
		KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
		// 클라이언트 키와 인증서가 들어있습니다.
		keyStore.load(null, null); // 키스토어 비번
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(keyStore, null); // 키 비번

		KeyStore trustKeyStore = KeyStore.getInstance("PKCS12");
		// CA 인증서가 들어있습니다.
		trustKeyStore.load(trustStoreInputStream,
				trustStorePassword); // 키 스토어 비번
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(trustKeyStore);

		SSLContext context = SSLContext.getInstance("TLSv1.3");
		context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

		return context;
	}

	public static SSLSocketFactory socketFactory(InputStream trustStoreInputStream, char[] trustStorePassword)
			throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException,
			KeyManagementException {
		return context(trustStoreInputStream, trustStorePassword).getSocketFactory();
	}
}
