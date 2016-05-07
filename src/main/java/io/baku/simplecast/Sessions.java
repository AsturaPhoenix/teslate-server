package io.baku.simplecast;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Sessions {
  private static final ConcurrentMap<String, Session> sessions = new ConcurrentHashMap<>();
  
  public static Session get(final String name) {
    return sessions.get(name);
  }
  
  public static Session getOrCreate(final String name) {
    Session s = sessions.get(name);
    if (s == null) {
      final Session candidate = new Session(name);
      s = sessions.putIfAbsent(name, candidate);
      if (s == null) {
        s = candidate;
      }
    }
    return s;
  }
}
