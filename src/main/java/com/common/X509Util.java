/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.common;

import com.quorum.util.ZKX509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static com.common.X509Exception.*;
/**
 * XXX: Borrowed from 3.5.x, modified to suite QuorumSocketFactory
 */
public class X509Util {
    private static final Logger LOG = LoggerFactory.getLogger(X509Util.class);

    public static final String SSL_VERSION = "TLSv1";
    public static final String KEYSTORE_LOCATION
            = "quorum.ssl.keyStore.location";
    public static final String KEYSTORE_PASSWD = "quorum.ssl.keyStore.password";
    public static final String TRUSTSTORE_LOCATION
            = "quorum.ssl.trustStore.location";
    public static final String TRUSTSTORE_PASSWD
            = "quorum.ssl.trustStore.password";
    public static final String TRUSTSTORE_CA_ALIAS
            = "quorum.ssl.trustStore.rootCA.alias";

    private final String sslVersion;
    private final String keyStoreLocation;
    private final String keyStorePassword;
    private final String trustStoreLocation;
    private final String trustStorePassword;
    private final String trustStoreCAAlias;

    final KeyManager[] keyManagers;
    final TrustManager[] trustManagers;

    public X509Util(final String sslVersion,
                    final String keyStoreLocation,
                    final String keyStorePassword,
                    final String trustStoreLocation,
                    final String trustStorePassword,
                    final String trustStoreCAAlias)
            throws NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException {
        this.sslVersion = sslVersion;
        this.keyStoreLocation = keyStoreLocation;
        this.keyStorePassword = keyStorePassword;
        this.trustStoreLocation = trustStoreLocation;
        this.trustStorePassword = trustStorePassword;
        this.trustStoreCAAlias = trustStoreCAAlias;

        validSSLVersion(this.sslVersion);
        keyManagers = initKeyManagers();
        trustManagers = initTrustManagers();
    }

    public X509Util(final String keyStoreLocation,
                    final String keyStorePassword,
                    final String trustStoreLocation,
                    final String trustStorePassword,
                    final String trustStoreCAAlias)
            throws NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException {
        this(SSL_VERSION, keyStoreLocation, keyStorePassword,
                trustStoreLocation, trustStorePassword, trustStoreCAAlias);
    }

    public X509Util(final String sslVersion)
            throws NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException {
        this(sslVersion, System.getProperty(KEYSTORE_LOCATION),
                System.getProperty(KEYSTORE_PASSWD),
                System.getProperty(TRUSTSTORE_LOCATION),
                System.getProperty(TRUSTSTORE_PASSWD),
                System.getProperty(TRUSTSTORE_CA_ALIAS));
    }

    public X509Util() throws NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException {
        this(SSL_VERSION);
    }

    private KeyManager[] initKeyManagers() throws KeyManagerException {
        boolean isValid;
        try {
            isValid = validKeyStoreArgs(keyStoreLocation, keyStorePassword);
        } catch (IllegalArgumentException exp) {
            LOG.error("failed to create key manager");
            throw new KeyManagerException(exp);
        }

        if (isValid) {
            return new KeyManager[] {
                    createKeyManager(keyStoreLocation, keyStorePassword) };
        } else {
            return null;
        }
    }

    private TrustManager[] initTrustManagers() throws TrustManagerException {
        boolean isValid;
        try {
            isValid = validKeyStoreArgs(trustStoreLocation,
                    trustStorePassword);
        } catch (IllegalArgumentException exp) {
            LOG.error("failed to create trust manager, illegal args: {}", exp);
            throw new TrustManagerException(exp);
        }

        if (isValid) {
            if (trustStoreCAAlias == null || trustStoreCAAlias.length() == 0) {
                final String errStr = "failed to create trust manager, " +
                        "no alias for: " + trustStoreLocation;
                LOG.error(errStr);
                throw new TrustManagerException(errStr);
            }
            return new TrustManager[] {
                    createTrustManager(trustStoreLocation, trustStorePassword,
                            trustStoreCAAlias) };
        } else {
            return null;
        }
    }

    private boolean validSSLVersion(final String sslVersion)
            throws NoSuchAlgorithmException {
        try {
            SSLContext.getInstance(sslVersion);
        } catch (NoSuchAlgorithmException exp) {
            LOG.error("Invalid SSL Version: {}, exp: {}", sslVersion, exp);
            throw exp;
        }

        return true;
    }

    private boolean validKeyStoreArgs(final String location,
                                         final String password)
            throws IllegalArgumentException {
        if (location == null && password == null) {
            LOG.warn("keystore not specified, default will be used");
            return false;
        } else {
            if (location == null) {
                throw new IllegalArgumentException("keystore location not " +
                        "specified");
            }
            if (password == null || password.length() == 0) {
                throw new IllegalArgumentException("keystore password not " +
                        "specified for: " + location);
            }
        }
        return true;
    }

    public SSLContext createSSLContext()
            throws SSLContextException {
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance(sslVersion);
            sslContext.init(keyManagers, trustManagers, null);
        } catch (NoSuchAlgorithmException exp) {
            LOG.error("invalid ssl version: {}", sslVersion);
            throw new SSLContextException(exp);
        } catch (KeyManagementException e) {
            throw new SSLContextException("Failed to init KeyManagers", e);
        }

        return sslContext;
    }

    public static X509KeyManager createKeyManager(
            final String keyStoreLocation, final String keyStorePassword)
            throws KeyManagerException {
        LOG.info("keyStoreLocation: {}", keyStoreLocation);
        KeyStore ks;
        try {
            ks = createKeyStore(keyStoreLocation, keyStorePassword);
            int size = ks.size();
            if (size > 1) {
                final String errStr = "Keystore should contain only one key, "
                        + "found: " + size + " for: " + keyStoreLocation;
                LOG.error(errStr);
                throw new KeyManagerException(errStr);
            }
        } catch (KeyStoreException | IOException |
                NoSuchAlgorithmException | CertificateException exp) {
            LOG.error("failed to create key manager due key store for: {},"
                    + "exp: {}", keyStoreLocation,  exp);
            throw new KeyManagerException(exp);
        }



        KeyManagerFactory kmf;
        try {
            kmf = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
            final char[] passwordChars
                    = keyStorePassword.toCharArray();
            kmf.init(ks, passwordChars);
        } catch (NoSuchAlgorithmException | KeyStoreException
                | UnrecoverableKeyException exp) {
            LOG.error("failed to create key manager while key store init for:"
                    + " {}, exp: {}", keyStoreLocation,  exp);
            throw new KeyManagerException(exp);
        }


        for (KeyManager km : kmf.getKeyManagers()) {
            if (km instanceof X509KeyManager) {
                return (X509KeyManager) km;
            }
        }

        final String errStr = "failed to create key manager, X509KeyManager "
                + "not available" + " for: " + keyStoreLocation + " with algo: "
                + KeyManagerFactory.getDefaultAlgorithm();
        LOG.error(errStr);
        throw new KeyManagerException(errStr);
    }

    public static X509TrustManager createTrustManager(
            final String trustStoreLocation, final String trustStorePassword,
            final String trustStoreCAAlias) throws TrustManagerException {
        LOG.info("TrustStoreLocation: {}", trustStoreLocation);
        try {
            KeyStore ts = createKeyStore(trustStoreLocation,
                    trustStorePassword);
            X509Certificate rootCA = getCertWithAlias(ts, trustStoreCAAlias);
            if (rootCA == null) {
                final String str = "failed to find root CA from: " +
                        trustStoreLocation + " with alias: " +
                        trustStoreCAAlias;
                LOG.error(str);
                throw new TrustManagerException(str);
            }

            return createTrustManager(rootCA);
        } catch (KeyStoreException | IOException |
                NoSuchAlgorithmException | CertificateException exp) {
            final String str = "failed to create trust manager from: " +
                    trustStoreLocation + " with alias: " +
                    trustStoreCAAlias;
            LOG.error(str);
            throw new TrustManagerException(exp);
        }
    }

    private static X509TrustManager createTrustManager(
            final X509Certificate rootCA) {
        return new ZKX509TrustManager(rootCA);
    }

    private static X509Certificate getCertWithAlias(
            final KeyStore trustStore, final String alias)
            throws KeyStoreException {
        X509Certificate cert;
        try {
            cert = (X509Certificate) trustStore.getCertificate(alias);
        } catch (KeyStoreException exp) {
            LOG.error("failed to load CA cert, exp: " + exp);
            throw exp;
        }

        return cert;
    }

    private static KeyStore createKeyStore(
            final String keyStoreLocation, final String keyStorePassword)
            throws KeyStoreException, IOException, NoSuchAlgorithmException,
            CertificateException {
        KeyStore ks;
        try(FileInputStream inputStream = new FileInputStream(
                new File(keyStoreLocation))) {
            final char[] passwordChars
                    = keyStorePassword.toCharArray();
            ks = KeyStore.getInstance("JKS");
            ks.load(inputStream, passwordChars);
        } catch (KeyStoreException | IOException |
                NoSuchAlgorithmException | CertificateException exp) {
            if (exp instanceof  IOException) {
                LOG.error("unable to load key store from: "
                        + keyStoreLocation + ", exp: " + exp);
            }
            throw exp;
        }

        return ks;
    }
}
