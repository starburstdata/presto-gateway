package com.lyft.data.gateway.ha.config;

import lombok.Data;

@Data
public class RequestRouterConfiguration {
  // Local gateway port
  private int port;

  // Name of the routing gateway name (for metrics purposes)
  private String name;

  // Use SSL?
  private boolean ssl;
  private String keystorePath;
  private String keystorePass;

  private int historySize = 2000;

  // Use the certificate between gateway and presto?
  private boolean forwardKeystore;

  // By default non-whitelisted requests are rerouted to the application port.
  // Set this to false to if separate networking rules are required  
  private boolean rerouteRequestsToApplication = true;
  // attempt to lookup unknown query Ids if true, otherwise rely
  // on recording them in the DB
  private boolean lookupQueryIds = true;
}
