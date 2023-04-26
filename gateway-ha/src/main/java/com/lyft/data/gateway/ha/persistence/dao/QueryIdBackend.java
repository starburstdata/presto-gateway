package com.lyft.data.gateway.ha.persistence.dao;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;

@IdName("queryid")
@Table("queryid")
public class QueryIdBackend extends Model {
  public static final String queryid = "queryid";
  public static final String backend = "backend";

  public static void create(
      QueryIdBackend model,
      String queryid,
      String backend) {
    model.set(QueryIdBackend.queryid, queryid);
    model.set(QueryIdBackend.backend, backend);
    model.insert();
  }
}
