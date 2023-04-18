package com.lyft.data.gateway.ha.persistence.dao;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Cached;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;

@IdName("ui_cookie")
@Table("ui_request")
@Cached
public class UiRequestBackend extends Model {
  public static final String uiCookie = "ui_cookie";
  public static final String backend = "backend";

  public static void create(
      UiRequestBackend model,
      String uiCookie,
      String backend) {
    model.set(UiRequestBackend.uiCookie, uiCookie);
    model.set(UiRequestBackend.backend, backend);
  }

}
