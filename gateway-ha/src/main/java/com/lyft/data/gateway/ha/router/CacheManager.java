package com.lyft.data.gateway.ha.router;

import com.lyft.data.gateway.ha.persistence.JdbcConnectionManager;
import com.lyft.data.gateway.ha.persistence.dao.QueryIdBackend;
import com.lyft.data.gateway.ha.persistence.dao.UiRequestBackend;
import lombok.extern.slf4j.Slf4j;

@Slf4j

public class CacheManager {
  private JdbcConnectionManager connectionManager;

  public CacheManager(JdbcConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }
  
  public void submitUiBackend(String uiCookie, String backend) {
    try {
      connectionManager.open();
      UiRequestBackend dao = new UiRequestBackend();
      log.debug(String.format("Writing cookie %s for backend %s", uiCookie, backend));
      UiRequestBackend.create(dao, uiCookie, backend);
    } catch (Exception e) {
      log.warn(String.format("Error saving cookie %s for backend %s: %s", uiCookie, backend, e));
    } finally {
      connectionManager.close();
    }
  }

  public void submitQueryIdBackend(String queryId, String backend) {
    try {
      connectionManager.open();
      QueryIdBackend dao = new QueryIdBackend();
      log.debug(String.format("Writing queryId %s for backend %s", queryId, backend));
      QueryIdBackend.create(dao, queryId, backend);
    } catch (Exception e) {
      log.warn(String.format("Error saving queryId %s for backend %s: %s", queryId, backend, e));
    } finally {
      connectionManager.close();
    }
  }

  public String getBackendForUiCookie(String uiCookie) {
    try {
      connectionManager.open();
      UiRequestBackend uiRequestBackend = UiRequestBackend.findById(uiCookie);
      return (String) uiRequestBackend.get(UiRequestBackend.backend);
    } finally {
      connectionManager.close();
    }
  }

  public String getBackendForQueryId(String queryId) {
    try {
      connectionManager.open();
      QueryIdBackend queryIdBackend = QueryIdBackend.findById(queryId);
      return (String) queryIdBackend.get(QueryIdBackend.backend);
    } finally {
      connectionManager.close();
    }
  }

  public boolean removeUiCookie(String uiCookie) {
    try {
      connectionManager.open();
      UiRequestBackend uiRequestBackend = UiRequestBackend.findById(uiCookie);
      return uiRequestBackend.delete();
    } finally {
      connectionManager.close();
    }
  }

  public boolean removeQueryId(String queryId) {
    try {
      connectionManager.open();
      QueryIdBackend queryIdBackend = QueryIdBackend.findById(queryId);
      return queryIdBackend.delete();
    } finally {
      connectionManager.close();
    }
  }
}
