package com.lyft.data.gateway.ha.router;

import com.lyft.data.gateway.ha.persistence.JdbcConnectionManager;
import com.lyft.data.gateway.ha.persistence.dao.UiRequestBackend;

public class UiManager {
  private JdbcConnectionManager connectionManager;

  public UiManager(JdbcConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }
  
  public void submitUiBackend(String uiCookie, String backend) {
    try {
      connectionManager.open();
      UiRequestBackend dao = new UiRequestBackend();
      UiRequestBackend.create(dao, uiCookie, backend);
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
}
