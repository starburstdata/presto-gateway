package com.lyft.data.gateway.ha.handler;

import com.codahale.metrics.Meter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.lyft.data.gateway.ha.router.QueryHistoryManager;
import com.lyft.data.gateway.ha.router.RoutingGroupSelector;
import com.lyft.data.gateway.ha.router.RoutingManager;
import com.lyft.data.proxyserver.ProxyHandler;
import com.lyft.data.proxyserver.wrapper.MultiReadHttpServletRequest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.client.api.Request;

import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.Callback;

@Slf4j
public class QueryIdCachingProxyHandler extends ProxyHandler {
  public static final String PROXY_TARGET_HEADER = "proxytarget";
  public static final String V1_STATEMENT_PATH = "/v1/statement";
  public static final String V1_QUERY_PATH = "/v1/query";
  public static final String V1_INFO_PATH = "/v1/info";
  public static final String UI_API_STATS_PATH = "/ui/api/stats";
  public static final String UI_API_QUEUED_LIST_PATH = "/ui/api/query?state=QUEUED";
  public static final String PRESTO_UI_PATH = "/ui";
  public static final String OAUTH2_PATH = "/oauth2";

  public static final String INSIGHTS_STATEMENT_PATH = "/ui/api/insights/ide/statement";
  public static final String USER_HEADER = "X-Trino-User";
  public static final String ALTERNATE_USER_HEADER = "X-Presto-User";
  public static final String SOURCE_HEADER = "X-Trino-Source";
  public static final String ALTERNATE_SOURCE_HEADER = "X-Presto-Source";
  public static final String HOST_HEADER = "Host";
  private static final int QUERY_TEXT_LENGTH_FOR_HISTORY = 200;
  private static final Pattern QUERY_ID_PATTERN = Pattern.compile(".*[/=?](\\d+_\\d+_\\d+_\\w+).*");

  private static final Pattern EXTRACT_BETWEEN_SINGLE_QUOTES = Pattern.compile("'([^\\s']+)'");

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final RoutingManager routingManager;
  private final RoutingGroupSelector routingGroupSelector;
  private final QueryHistoryManager queryHistoryManager;

  private final Meter requestMeter;
  private final int serverApplicationPort;

  private final Map<String, String> sessionBackendMap = new HashMap<>();
  private final Map<Integer, String> requestIdBackendMap = new HashMap<>();

  public QueryIdCachingProxyHandler(
      QueryHistoryManager queryHistoryManager,
      RoutingManager routingManager,
      RoutingGroupSelector routingGroupSelector,
      int serverApplicationPort,
      Meter requestMeter) {
    this.requestMeter = requestMeter;
    this.routingManager = routingManager;
    this.routingGroupSelector = routingGroupSelector;
    this.queryHistoryManager = queryHistoryManager;
    this.serverApplicationPort = serverApplicationPort;
  }

  @Override
  public void preConnectionHook(HttpServletRequest request, Request proxyRequest) {
    log.debug("Enter pre conection hook");
    if (request.getMethod().equals(HttpMethod.POST)
        && request.getRequestURI().startsWith(V1_STATEMENT_PATH)) {
      requestMeter.mark();
      try {
        String requestBody = CharStreams.toString(request.getReader());
        log.info(
            "Processing request endpoint: [{}], payload: [{}]",
            request.getRequestURI(),
            requestBody);
        debugLogHeaders(request);
      } catch (Exception e) {
        log.warn("Error fetching the request payload", e);
      }
    }

    if (isPathWhiteListed(request.getRequestURI())) {
      setForwardedHostHeaderOnProxyRequest(request, proxyRequest);
    }
  }

  private boolean isPathWhiteListed(String path) {
    return path.startsWith(V1_STATEMENT_PATH)
        || path.startsWith(V1_QUERY_PATH)
        || path.startsWith(PRESTO_UI_PATH)
        || path.startsWith(V1_INFO_PATH)
        || path.startsWith(UI_API_STATS_PATH)
        || path.startsWith(OAUTH2_PATH);
  }

  public boolean isAuthEnabled() {
    return false;
  }

  public boolean handleAuthRequest(HttpServletRequest request) {
    return true;
  }

  @Override
  public String rewriteTarget(HttpServletRequest request, int requestId) {
    log.debug("Enter rewriteTarget");
    /* Here comes the load balancer / gateway */
    String backendAddress = "http://localhost:" + serverApplicationPort;
    // Only load balance presto query APIs.
    if (isPathWhiteListed(request.getRequestURI())) {
      String queryId = extractQueryIdIfPresent(request);
      if (!Strings.isNullOrEmpty(queryId)) {
        backendAddress = routingManager.findBackendForQueryId(queryId);
      } else {
        if (!Strings.isNullOrEmpty(request.getRequestedSessionId())
            && !(request.getRequestURI().startsWith(V1_STATEMENT_PATH)
                || request.getRequestURI().startsWith(V1_STATEMENT_PATH))) {
          //pin browser sessions to the same backend, but load balance queries
          backendAddress = sessionBackendMap.get(request.getRequestedSessionId().split("\\.")[0]);
          if (Strings.isNullOrEmpty(backendAddress)) {
            log.error("Unknown jessionid: " + request.getRequestedSessionId());
            backendAddress = getBackendForRequest(request);
          }
        } else {
          backendAddress = getBackendForRequest(request);
          sessionBackendMap.put(request.getSession().getId(), backendAddress);
          log.info("using session id " + request.getSession().getId());
          if (request.getRequestURI().startsWith(V1_STATEMENT_PATH)
                  || request.getRequestURI().startsWith(V1_STATEMENT_PATH)) {
            requestIdBackendMap.put(requestId, backendAddress);
          }
        }
      }
    }
    if (isAuthEnabled() && request.getHeader("Authorization") != null) {
      if (!handleAuthRequest(request)) {
        // This implies the AuthRequest was not authenticated, hence we error out from here.
        log.info("Could not authenticate Request: " + request.toString());
        return null;
      }
    }
    String targetLocation =
            backendAddress
                    + request.getRequestURI()
                    + (request.getQueryString() != null ? "?" + request.getQueryString() : "");

    String originalLocation =
            request.getScheme()
                    + "://"
                    + request.getRemoteHost()
                    + ":"
                    + request.getServerPort()
                    + request.getRequestURI()
                    + (request.getQueryString() != null ? "?" + request.getQueryString() : "");
    log.info("Rerouting [{}]--> [{}]", originalLocation, targetLocation);
    return targetLocation;
  }

  String getBackendForRequest(HttpServletRequest request) {
    String routingGroup = routingGroupSelector.findRoutingGroup(request);
    String user = Optional.ofNullable(request.getHeader(USER_HEADER))
            .orElse(request.getHeader(ALTERNATE_USER_HEADER));
    if (!Strings.isNullOrEmpty(routingGroup)) {
      // This falls back on adhoc backend if there are no cluster found for the routing group.
      return routingManager.provideBackendForRoutingGroup(routingGroup, user);
    } else {
      return routingManager.provideAdhocBackend(user);
    }
  }

  protected String extractQueryIdIfPresent(HttpServletRequest request) {
    String path = request.getRequestURI();
    String queryParams = request.getQueryString();
    try {
      String queryText = CharStreams.toString(request.getReader());
      if (!Strings.isNullOrEmpty(queryText)
          && queryText.toLowerCase().contains("system.runtime.kill_query")) {
        // extract and return the queryId
        String[] parts = queryText.split(",");
        for (String part : parts) {
          if (part.contains("query_id")) {
            Matcher m = EXTRACT_BETWEEN_SINGLE_QUOTES.matcher(part);
            if (m.find()) {
              String queryQuoted = m.group();
              if (!Strings.isNullOrEmpty(queryQuoted) && queryQuoted.length() > 0) {
                return queryQuoted.substring(1, queryQuoted.length() - 1);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Error extracting query payload from request", e);
    }

    return extractQueryIdIfPresent(path, queryParams);
  }

  protected static String extractQueryIdIfPresent(String path, String queryParams) {
    if (path == null) {
      return null;
    }
    String queryId = null;

    log.debug("trying to extract query id from  path [{}] or queryString [{}]", path, queryParams);
    if (path.startsWith(V1_STATEMENT_PATH) || path.startsWith(V1_QUERY_PATH)) {
      String[] tokens = path.split("/");
      if (tokens.length >= 4) {
        if (path.contains("queued")
            || path.contains("scheduled")
            || path.contains("executing")
            || path.contains("partialCancel")) {
          queryId = tokens[4];
        } else {
          queryId = tokens[3];
        }
      }
    } else if (path.startsWith(PRESTO_UI_PATH)) {
      Matcher matcher = QUERY_ID_PATTERN.matcher(path);
      if (matcher.matches()) {
        queryId = matcher.group(1);
      }
    }
    log.debug("query id in url [{}]", queryId);
    return queryId;
  }

  private boolean doRecordQueryId(HttpServletRequest request) {
    String requestPath = request.getRequestURI();
    return (requestPath.startsWith(V1_STATEMENT_PATH)
            || requestPath.startsWith(INSIGHTS_STATEMENT_PATH))
            && request.getMethod().equals(HttpMethod.POST);
  }

  protected void postConnectionHook(
      HttpServletRequest request,
      HttpServletResponse response,
      byte[] buffer,
      int offset,
      int length,
      Callback callback,
      int requestId) {
    log.debug("Enter post conection hook");
    log.debug("URI: " + request.getRequestURI());
    try {
      if (doRecordQueryId(request)) {
        recordBackendForQueryId(request, response, buffer, requestId);
      } else {
        log.debug("SKIPPING For {}", request.getRequestURI());
      }
    } catch (Exception e) {
      log.error("Error in proxying falling back to super call", e);
    }
    super.postConnectionHook(request, response, buffer, offset, length, callback);
  }

  void recordBackendForQueryId(
      HttpServletRequest request,
      HttpServletResponse response,
      byte[] buffer,
      int requestId)
      throws IOException {
    String output;
    boolean isGZipEncoding = isGZipEncoding(response);
    if (isGZipEncoding) {
      output = plainTextFromGz(buffer);
    } else {
      output = new String(buffer);
    }
    log.debug("For Request [{}] got Response output [{}]", request.getRequestURI(), output);
    log.debug("Request Id: " + requestId);

    QueryHistoryManager.QueryDetail queryDetail = getQueryDetailsFromRequest(request);
    //TODO: use the requestId or something else that changes per query,
    // since this will only use one backend
    String backendUrl = Strings.isNullOrEmpty(queryDetail.getBackendUrl())
            ? requestIdBackendMap.get(requestId)
            : queryDetail.getBackendUrl();
    log.debug("Extracting Proxy destination : [{}] for request : [{}]",
            backendUrl, request.getRequestURI());

    if (response.getStatus() == HttpStatus.OK_200) {
      HashMap<String, String> results = OBJECT_MAPPER.readValue(output, HashMap.class);
      queryDetail.setQueryId(results.get("id"));

      if (!Strings.isNullOrEmpty(queryDetail.getQueryId())) {
        //TODO: use the DB to back the queryId cache so it is shared across gateway instances
        routingManager.setBackendForQueryId(
                queryDetail.getQueryId(), backendUrl);
        log.debug(
                "QueryId [{}] mapped with proxy [{}]",
                queryDetail.getQueryId(),
                backendUrl);
        requestIdBackendMap.remove(requestId);
      } else {
        log.debug("QueryId [{}] could not be cached", queryDetail.getQueryId());
      }
    } else {
      log.error(
              "Non OK HTTP Status code with response [{}] , Status code [{}]",
              output,
              response.getStatus());
    }
    // Saving history at gateway.
    queryHistoryManager.submitQueryDetail(queryDetail);
  }

  static void setForwardedHostHeaderOnProxyRequest(HttpServletRequest request,
                                                   Request proxyRequest) {
    if (request.getHeader(PROXY_TARGET_HEADER) != null) {
      try {
        URI backendUri = new URI(request.getHeader(PROXY_TARGET_HEADER));
        StringBuilder hostName = new StringBuilder();
        hostName.append(backendUri.getHost());
        if (backendUri.getPort() != -1) {
          hostName.append(":").append(backendUri.getPort());
        }
        String overrideHostName = hostName.toString();
        log.debug("Incoming Request Host header : [{}], proxy request host header : [{}]",
            request.getHeader(HOST_HEADER), overrideHostName);

        proxyRequest.header(HOST_HEADER, overrideHostName);
      } catch (URISyntaxException e) {
        log.warn(e.toString());
      }
    } else {
      log.warn("Proxy Target not set on request, unable to decipher HOST header");
    }
  }

  private QueryHistoryManager.QueryDetail getQueryDetailsFromRequest(HttpServletRequest request)
      throws IOException {
    QueryHistoryManager.QueryDetail queryDetail = new QueryHistoryManager.QueryDetail();
    queryDetail.setBackendUrl(request.getHeader(PROXY_TARGET_HEADER));
    queryDetail.setCaptureTime(System.currentTimeMillis());
    queryDetail.setUser(Optional.ofNullable(request.getHeader(USER_HEADER))
            .orElse(request.getHeader(ALTERNATE_USER_HEADER)));
    queryDetail.setSource(Optional.ofNullable(request.getHeader(SOURCE_HEADER))
            .orElse(request.getHeader(ALTERNATE_SOURCE_HEADER)));
    String queryText = CharStreams.toString(request.getReader());
    queryDetail.setQueryText(
        queryText.length() > QUERY_TEXT_LENGTH_FOR_HISTORY
            ? queryText.substring(0, QUERY_TEXT_LENGTH_FOR_HISTORY) + "..."
            : queryText);
    return queryDetail;
  }
}
