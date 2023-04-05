package com.lyft.data.gateway.ha.router;

import java.io.FileNotFoundException;
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
  String rulesConfigPath;
  Rules rules;
  boolean rulesLoaded;
  long lastUpdatedTime;

  RuleCachingRoutingGroupSelector(String rulesConfigPath) {
    this.rulesConfigPath = rulesConfigPath;

    try {
      rules = ruleFactory.createRules(
              new FileReader(rulesConfigPath));
      BasicFileAttributes attr = Files.readAttributes(Path.of(rulesConfigPath),
              BasicFileAttributes.class);
      lastUpdatedTime = attr.lastModifiedTime().toMillis();

      rulesLoaded = true;
    } catch (Exception e) {
      log.error("Error opening rules configuration file, using "
              + "routing group header as default.", e);
    }
  }

  @Override
  public String findRoutingGroup(HttpServletRequest request) {
    try {
      BasicFileAttributes attr = Files.readAttributes(Path.of(rulesConfigPath),
              BasicFileAttributes.class);
      log.debug(String.format("Current modified time %s, last modified time %s",
              attr.lastModifiedTime().toMillis(), lastUpdatedTime));
      if (attr.lastModifiedTime().toMillis() > lastUpdatedTime) {
        log.info(String.format("Updating rules to file modified at %s", attr.lastModifiedTime()));
        synchronized (this) {
          // TODO: talk to the guys about whether or not we should synchronize
          //  on this, rules, or sth else
          rules = ruleFactory.createRules(
                  new FileReader(rulesConfigPath));
          lastUpdatedTime = attr.lastModifiedTime().toMillis();
        }
      }
      rulesLoaded = true;
    } catch (Exception e) {
      log.error("Error opening rules configuration file, using "
              + "routing group header as default.", e);
      rulesLoaded = false;
      // This could lead to perf problems as every thread goes into the synchronized
      // block until the issue is resolved
    }
    if (rulesLoaded) {
      Facts facts = new Facts();
      HashMap<String, String> result = new HashMap<String, String>();
      facts.put("request", request);
      facts.put("result", result);
      rulesEngine.fire(rules, facts);
      return result.get("routingGroup");
    }
    return Optional.ofNullable(request.getHeader(ROUTING_GROUP_HEADER))
        .orElse(request.getHeader(ALTERNATE_ROUTING_GROUP_HEADER));
  }
}
