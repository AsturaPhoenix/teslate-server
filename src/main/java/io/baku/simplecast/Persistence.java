package io.baku.simplecast;

import java.util.logging.Level;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.appengine.api.memcache.ConsistentLogAndContinueErrorHandler;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheService.IdentifiableValue;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

import lombok.experimental.UtilityClass;
import lombok.extern.java.Log;

@Log
@UtilityClass
public class Persistence {
  private static final int
      FRAME_POLL_PERIOD = 50,
      FRAME_POLL_TIMEOUT = 2500,
      COMMAND_POLL_PERIOD = 50,
      COMMAND_POLL_TIMEOUT = 10000;
  
  private static final MemcacheService cache = MemcacheServiceFactory.getMemcacheService();
  static {
    cache.setErrorHandler(new ConsistentLogAndContinueErrorHandler(Level.WARNING));
  }
  
  public static byte[] getImageBytes(final String name, final String variant) {
    return (byte[])cache.get(name + "/" + variant);
  }
  
  public static byte[] awaitAuditedImageBytes(final String name, final String variant) {
    //Poll for changes because memcache doesn't expose distributed events
    for (int i = 0; i < FRAME_POLL_TIMEOUT / FRAME_POLL_PERIOD &&
        getImageLastAccessed(name,  variant) > getImageLastModified(name, variant); i++) {
      try {
        Thread.sleep(FRAME_POLL_PERIOD);
      } catch (final InterruptedException e) {
        log.warning(Throwables.getStackTraceAsString(e));
        break;
      }
    }
    
    cache.put(name + "/" + variant + "/last-accessed", System.currentTimeMillis());
    return getImageBytes(name, variant);
  }
  
  public static void putAuditedImage(final String name, final String variant, final byte[] bytes) {
    final String key = name + "/" + variant;
    cache.put(key, bytes);
    cache.put(key + "/last-modified", System.currentTimeMillis());
  }
  
  private static long getImageLastAccessed(final String name, final String variant) {
    return readLong(name + "/" + variant + "/last-accessed");
  }
  
  public static long getImageLastModified(final String name, final String variant) {
    return readLong(name + "/" + variant + "/last-modified");
  }
  
  private static long readLong(final String key) {
    final Long value = (Long)cache.get(key);
    return value == null? Long.MIN_VALUE : value;
  }
  
  public static void pushCommand(final String name, final byte[] command) {
    cache.put(name + "/command", command);
  }
  
  public static byte[] popCommand(final String name) {
    final String key = name + "/command";

    IdentifiableValue c = cache.getIdentifiable(key);
    
    //Poll for changes because memcache doesn't expose distributed events
    for (int i = 0; i < COMMAND_POLL_TIMEOUT / COMMAND_POLL_PERIOD &&
        (c == null || c.getValue() == null); i++) {
      try {
        Thread.sleep(COMMAND_POLL_PERIOD);
        c = cache.getIdentifiable(key);
      } catch (final InterruptedException e) {
        log.warning(Throwables.getStackTraceAsString(e));
        break;
      }
    }
    if (c != null && c.getValue() != null) {
      cache.putIfUntouched(key, c, null);
    }
    return c == null? null : (byte[])c.getValue();
  }
}
