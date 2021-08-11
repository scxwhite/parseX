package com.sucx.util;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/26
 */
public class HttpUtils {


    public static String doGet(String urlStr, List<BasicHeader> headers) {
        return doExecute(urlStr, new HttpGet(check(urlStr)), headers);
    }

    public static String doPost(String urlStr, List<BasicHeader> headers) {
        return doExecute(urlStr, new HttpPost(check(urlStr)), headers);
    }

    private static String doExecute(String url, HttpUriRequest request, List<BasicHeader> headers) {
        if (headers != null && headers.size() > 0) {
            for (BasicHeader header : headers) {
                request.setHeader(header);
            }
        }
        SSLConnectionSocketFactory scsf = null;
        try {
            scsf = new SSLConnectionSocketFactory(
                    SSLContexts.custom().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build(),
                    NoopHostnameVerifier.INSTANCE);
        } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
            e.printStackTrace();
        }
        try {
            HttpClient httpClient = HttpClients.custom().setSSLSocketFactory(scsf).build();
            HttpResponse response = httpClient.execute(request);
            int code = response.getStatusLine().getStatusCode();
            if (code == 200) {
                return EntityUtils.toString(response.getEntity());
            } else {
                throw new RuntimeException(url + " http请求异常:" + response.getStatusLine().getStatusCode() + response.getEntity().toString());
            }
        } catch (IOException e) {
            throw new RuntimeException("发送http请求失败", e);
        }
    }


    private static URI check(String urlStr) {
        URI uri = null;
        try {
            URL url = new URL(urlStr);
            uri = new URI(url.getProtocol(), url.getHost() + ":" + url.getPort(), url.getPath(), url.getQuery(), null);
            return uri;
        } catch (URISyntaxException | MalformedURLException e) {
            return null;
        }

    }


}
