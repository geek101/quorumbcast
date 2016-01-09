package com.quorum.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.net.Socket;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Created by powell on 12/9/15.
 */
public class MyX509ExtendedTrustManager extends X509ExtendedTrustManager {
    private static final Logger LOG
            = LoggerFactory.getLogger(MyX509ExtendedTrustManager.class);
    /*
     * The default PKIX X509ExtendedTrustManager.  We'll delegate
     * decisions to it, and fall back to the logic in this class if the
     * default X509ExtendedTrustManager doesn't trust it.
     */
    private final X509ExtendedTrustManager pkixTrustManager;
    private final X509Certificate rootCACert;

    public MyX509ExtendedTrustManager(final KeyStore keyStore,
                               final X509Certificate rootCACert)
            throws KeyStoreException,
            NoSuchAlgorithmException, CertificateException, IOException {
        // create a "default" JSSE X509ExtendedTrustManager.

        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance("PKIX");
        tmf.init(keyStore);

        TrustManager tms [] = tmf.getTrustManagers();

        this.rootCACert = rootCACert;

         /*
          * Iterate over the returned trustmanagers, look
          * for an instance of X509TrustManager.  If found,
          * use that as our "default" trust manager.
          */
        for (int i = 0; i < tms.length; i++) {
            if (tms[i] instanceof X509ExtendedTrustManager) {
                pkixTrustManager = (X509ExtendedTrustManager) tms[i];
                return;
            }
        }

         /*
          * Find some other way to initialize, or else we have to fail the
          * constructor.
          */
        throw new KeyStoreException("Couldn't initialize with trustStore");
    }

    /*
     * Delegate to the default trust manager.
     */
    public void checkClientTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        try {
            pkixTrustManager.checkClientTrusted(chain, authType);
        } catch (CertificateException exp) {
            LOG.error("pkix check client failed, exp: " + exp);
            throw exp;
        }

        checkAllCerts(chain);
    }

    /*
     * Delegate to the default trust manager.
     */
    public void checkServerTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        try {
            pkixTrustManager.checkServerTrusted(chain, authType);
        } catch (CertificateException exp) {
            LOG.error("pkix check server failed, exp: " + exp);
            throw exp;
        }

        checkAllCerts(chain);
    }

    /*
     * Connection-sensitive verification.
     */
    public void checkClientTrusted(X509Certificate[] chain, String authType,
                                   Socket socket)
            throws CertificateException {
        try {
            pkixTrustManager.checkClientTrusted(chain, authType, socket);
        } catch (CertificateException exp) {
            LOG.error("pkix check client failed, exp: " + exp);
            throw exp;
        }

        checkAllCerts(chain);
    }

    public void checkClientTrusted(X509Certificate[] chain, String authType,
                                   SSLEngine engine)
            throws CertificateException {
        try {
            pkixTrustManager.checkClientTrusted(chain, authType, engine);
        } catch (CertificateException exp) {
            LOG.error("pkix check client failed, exp: " + exp);
            throw exp;
        }

        checkAllCerts(chain);
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType,
                                   Socket socket)
            throws CertificateException {
        try {
            pkixTrustManager.checkServerTrusted(chain, authType, socket);
        } catch (CertificateException exp) {
            LOG.error("pkix check server failed, exp: " + exp);
            throw exp;
        }

        checkAllCerts(chain);
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType,
                                   SSLEngine engine)
            throws CertificateException {
        try {
            pkixTrustManager.checkServerTrusted(chain, authType, engine);
        } catch (CertificateException exp) {
            LOG.error("pkix check server failed, exp: " + exp);
            throw exp;
        }

        checkAllCerts(chain);
    }

    /*
     * Merely pass this through.
     */
    public X509Certificate[] getAcceptedIssuers() {
        return pkixTrustManager.getAcceptedIssuers();
    }

    private void checkAllCerts(final X509Certificate[] certs)
            throws CertificateException {
        for (X509Certificate cert : certs) {
            try {
                cert.verify(rootCACert.getPublicKey());
            } catch (NoSuchAlgorithmException
                    | InvalidKeyException | NoSuchProviderException
                    | SignatureException exp) {
                LOG.error("cert validation failed, exp: " + exp);
                throw new CertificateException(exp);
            }
        }
    }
}
