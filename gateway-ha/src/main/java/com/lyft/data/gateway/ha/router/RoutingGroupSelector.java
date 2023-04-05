package com.lyft.data.gateway.ha.router;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.mvel.MVELRuleFactory;
import org.jeasy.rules.support.reader.YamlRuleDefinitionReader;

/** RoutingGroupSelector provides a way to match an HTTP request to a Gateway routing group. */
public interface RoutingGroupSelector {
  String ROUTING_GROUP_HEADER = "X-Trino-Routing-Group";
  String ALTERNATE_ROUTING_GROUP_HEADER = "X-Presto-Routing-Group";

  /**
   * Routing group selector that relies on the X-Trino-Routing-Group or X-Presto-Routing-Group
   * header to determine the right routing group.
   */
  static RoutingGroupSelector byRoutingGroupHeader() {
    return request -> Optional.ofNullable(request.getHeader(ROUTING_GROUP_HEADER))
            .orElse(request.getHeader(ALTERNATE_ROUTING_GROUP_HEADER));
  }

  /**
   * Routing group selector that uses routing engine rules
   * to determine the right routing group.
   */
  static RoutingGroupSelector byRoutingRulesEngine(String rulesConfigPath) {
    return new RuleCachingRoutingGroupSelector(rulesConfigPath);
  }

  /**
   * Given an HTTP request find a routing group to direct the request to. If a routing group cannot
   * be determined return null.
   */
  String findRoutingGroup(HttpServletRequest request);
}
