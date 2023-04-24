package com.lyft.data.gateway.ha.router;

import com.lyft.data.gateway.ha.persistence.JdbcConnectionManager;
import com.lyft.data.gateway.ha.persistence.dao.UiRequestBackend;
import lombok.extern.slf4j.Slf4j;

@Slf4j

public class UiManager {
  private JdbcConnectionManager connectionManager;

  public UiManager(JdbcConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }
  
  public void submitUiBackend(String uiCookie, String backend) {
    try {
      connectionManager.open();
      UiRequestBackend dao = new UiRequestBackend();
      log.info(String.format("Writing cookie %s for backend %s", uiCookie, backend));
      UiRequestBackend.create(dao, uiCookie, backend);
    } catch (Exception e) {
      log.warn(String.format("Error saving cookie %s for backend %s: %s", uiCookie, backend, e));
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

  public boolean removeUiCookie(String uiCookie) {
    try {
      connectionManager.open();
      UiRequestBackend uiRequestBackend = UiRequestBackend.findById(uiCookie);
      return uiRequestBackend.delete();
    } finally {
      connectionManager.close();
    }
  }
}
