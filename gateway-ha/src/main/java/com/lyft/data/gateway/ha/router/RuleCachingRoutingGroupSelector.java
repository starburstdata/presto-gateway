package com.lyft.data.gateway.ha.router;

import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
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
  Rules rules;
  FileTime lastUpdatedTime;

  RuleCachingRoutingGroupSelector(String rulesConfigPath) {
    try {
      BasicFileAttributes attr = Files.readAttributes(Path.of(rulesConfigPath),
              BasicFileAttributes.class);
      lastUpdatedTime = attr.lastModifiedTime();
      rules = ruleFactory.createRules(
              new FileReader(rulesConfigPath));
    } catch (Exception e) {
      log.error("Error opening rules configuration file, using "
              + "routing group header as default.", e);
    }
  }

  @Override
  public String findRoutingGroup(HttpServletRequest request) {
    Facts facts = new Facts();
    HashMap<String, String> result = new HashMap<String, String>();
    facts.put("request", request);
    facts.put("result", result);
    rulesEngine.fire(rules, facts);
    return result.get("routingGroup");
  }
}
