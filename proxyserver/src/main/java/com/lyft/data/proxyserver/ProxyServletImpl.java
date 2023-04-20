package com.lyft.data.proxyserver;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.extern.slf4j.Slf4j;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.ssl.SslContextFactory;

@Slf4j
public class ProxyServletImpl extends ProxyServlet.Transparent {
  private ProxyHandler proxyHandler;
  private ProxyServerConfiguration serverConfig;
  private final Map<String, String> trinoNonceBackendMap = new HashMap<>();
  private final Map<Integer, String> idBackendMap = new HashMap<>();

  public void setProxyHandler(ProxyHandler proxyHandler) {
    this.proxyHandler = proxyHandler;
    // This needs to be high as external clients may take longer to connect.
    this.setTimeout(TimeUnit.MINUTES.toMillis(1));
  }

  public void setServerConfig(ProxyServerConfiguration config) {
    this.serverConfig = config;
  }

  // Overriding this method to support ssl
  @Override
  protected HttpClient newHttpClient() {
    SslContextFactory sslFactory = new SslContextFactory();

    if (serverConfig != null && serverConfig.isForwardKeystore()) {
      sslFactory.setKeyStorePath(serverConfig.getKeystorePath());
      sslFactory.setKeyStorePassword(serverConfig.getKeystorePass());
    } else {
      sslFactory.setTrustAll(true);
    }
    sslFactory.setStopTimeout(TimeUnit.SECONDS.toMillis(15));
    sslFactory.setSslSessionTimeout((int) TimeUnit.SECONDS.toMillis(15));

    HttpClient httpClient = new HttpClient(sslFactory);
    httpClient.setMaxConnectionsPerDestination(10000);
    httpClient.setConnectTimeout(TimeUnit.SECONDS.toMillis(60));
    return httpClient;
  }

  /** Customize the headers of forwarding proxy requests. */
  @Override
  protected void addProxyHeaders(HttpServletRequest request, Request proxyRequest) {
    super.addProxyHeaders(request, proxyRequest);
    if (proxyHandler != null) {
      proxyHandler.preConnectionHook(request, proxyRequest);
    }
  }

  @Override
  protected String rewriteTarget(HttpServletRequest request) {
    String target = null;
    /*
    if (request.getCookies() != null && Arrays.stream(request.getCookies()).anyMatch(
        cookie -> cookie.getName().equals("__Secure-Trino-Nonce"))) {
      target = trinoNonceBackendMap.get(
        Arrays.stream(
                request.getCookies()).filter(
                    cookie -> cookie.getName().equals("__Secure-Trino-Nonce")).findAny());

      if (target == null) {
        log.error("_Secure-Trino-Nonce is set but wasn't stored. OAuth login may fail.");
      } else {
        log.debug("Using nonce routing");
        return target;
      }
    }

     */
    if (proxyHandler != null) {
      target = proxyHandler.rewriteTarget(request);
    }
    if (target == null) {
      target = super.rewriteTarget(request);
    }
    idBackendMap.put(this.getRequestId(request), target);
    log.debug("Target : " + target);
    return target;
  }

  @Override
  protected void onServerResponseHeaders(
          HttpServletRequest clientRequest,
          HttpServletResponse proxyResponse,
          Response serverResponse) {
    HttpFields serverHeaders = serverResponse.getHeaders();
    log.debug("Server headers: " + serverHeaders.toString());

    for (String header : proxyResponse.getHeaderNames()) {
      log.debug("proxy response header: " + header);
    }
    if (clientRequest.getRequestURI().equals("/ui/api/insights/logout")) {
      String setCookie = serverHeaders.get("Set-Cookie");
      serverHeaders.remove("Set-Cookie");
      serverHeaders.add("Set-Cookie", "JSESSIONID=delete;" + setCookie);
    }
    /*
    if (serverHeaders.containsKey("Set-Cookie")) {
      // check if request contained ui token or not
      String setCookie = serverHeaders.get("Set-Cookie");
      log.info("Proxy Response has Set-Cookie: " + setCookie);
      if (setCookie.indexOf("__Secure-Trino-Nonce") > -1) {
        String[] cookies = setCookie.split(";");
        for (String cookie : cookies) {
          String name = cookie.split("=")[0];
          String value = cookie.split("=")[1];
          if (name.equals("__Secure-Trino-Nonce")) {
            if (value.equals("delete")) {
              log.info("deleting nonce from cache");
              trinoNonceBackendMap.remove(value);
            } else {
              log.info("Added nonce " + value + " for backend "
                      + idBackendMap.get(this.getRequestId(clientRequest)));
              trinoNonceBackendMap.put(
                      cookie.split(";")[1],
                      idBackendMap.get(this.getRequestId(clientRequest)));
            }
          }
        }
      }
    }
    */
    super.onServerResponseHeaders(clientRequest, proxyResponse, serverResponse);
  }

  /**
   * Customize the response returned from remote server.
   *
   * @param request
   * @param response
   * @param proxyResponse
   * @param buffer
   * @param offset
   * @param length
   * @param callback
   */
  protected void onResponseContent(
      HttpServletRequest request,
      HttpServletResponse response,
      Response proxyResponse,
      byte[] buffer,
      int offset,
      int length,
      Callback callback) {
    try {
      if (this._log.isDebugEnabled()) {
        this._log.debug(
            "[{}] proxying content to downstream: [{}] bytes", this.getRequestId(request), length);
      }
      if (this.proxyHandler != null) {
        proxyHandler.postConnectionHook(request, response, buffer, offset, length, callback);
      } else {
        super.onResponseContent(request, response, proxyResponse, buffer, offset, length, callback);
      }
    } catch (Throwable var9) {
      callback.failed(var9);
    }
  }
}
