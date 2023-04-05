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

@Slf4j
public class RuleCachingRoutingGroupSelector
    implements RoutingGroupSelector  {
  // TODO: This is only a refactor, the same logic is implemented. Test this,
  //  then implement logic to only load new rules if the rules file is updated,
  //  and synchronize the update. First you could put
  // the rules factory creation in the constructor as a test.

  RulesEngine rulesEngine = new DefaultRulesEngine();
  MVELRuleFactory ruleFactory = new MVELRuleFactory(new YamlRuleDefinitionReader());
  String rulesConfigPath;

  RuleCachingRoutingGroupSelector(String rulesConfigPath) {
    this.rulesConfigPath = rulesConfigPath;
  }

  @Override
  public String findRoutingGroup(HttpServletRequest request) {
    try {
      Rules rules = ruleFactory.createRules(
          new FileReader(rulesConfigPath));
      Facts facts = new Facts();
      HashMap<String, String> result = new HashMap<String, String>();
      facts.put("request", request);
      facts.put("result", result);
      rulesEngine.fire(rules, facts);
      return result.get("routingGroup");
    } catch (Exception e) {
      log.error("Error opening rules configuration file, using "
            + "routing group header as default.", e);
      return Optional.ofNullable(request.getHeader(ROUTING_GROUP_HEADER))
          .orElse(request.getHeader(ALTERNATE_ROUTING_GROUP_HEADER));
    }
  }
}
