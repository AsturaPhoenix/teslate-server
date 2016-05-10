package io.baku.simplecast;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;

import javax.annotation.Nullable;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.appengine.api.datastore.AsyncDatastoreService;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.memcache.ConsistentLogAndContinueErrorHandler;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheService.IdentifiableValue;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.common.util.concurrent.Uninterruptibles;

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
  
  private static final String
    SESSION_KIND = "Session",
    FRAME_KIND = "Frame",
    BYTES_PROP = "bytes",
    METADATA_KIND = "Metadata",
    LAST_ACCESSED_KEY = "lastAccessed",
    LAST_MODIFIED_KEY = "lastModified",
    VALUE_PROP = "value";
     
  
  private static final MemcacheService cache = MemcacheServiceFactory.getMemcacheService();
  static {
    cache.setErrorHandler(new ConsistentLogAndContinueErrorHandler(Level.WARNING));
  }
  
  private static final AsyncDatastoreService datastore = DatastoreServiceFactory.getAsyncDatastoreService();
  
  private static Key createKey(final String name, final String variant) {
    return KeyFactory.createKey(SESSION_KIND, name).getChild(FRAME_KIND, variant);
  }
  
  public static byte[] getImageBytes(final String name, final String variant) {
    final byte[] cached = (byte[])cache.get(name + "/" + variant);
    if (cached != null) {
      return cached;
    }
    
    try {
      return ((Blob)Uninterruptibles.getUninterruptibly(datastore.get(createKey(name, variant)))
          .getProperty(BYTES_PROP)).getBytes();
    } catch (final ExecutionException e) {
      log.warning("Session " + name + ": no frame for " + variant + " in datastore.");
      return null;
    }
  }
  
  public static void putImageBytes(final String name, final String variant, final byte[] bytes) {
    cache.put(name + "/" + variant, bytes);
    final Entity frame = new Entity(createKey(name, variant));
    frame.setUnindexedProperty(BYTES_PROP, new Blob(bytes));
    datastore.put(frame);
  }
  
  public static byte[] awaitAuditedImageBytes(final String name, final String variant) {
    //Poll for changes because memcache doesn't expose distributed events
    final Key lastAccessedKey = createKey(name, variant).getChild(METADATA_KIND, LAST_ACCESSED_KEY);
    
    for (int i = 0; i < FRAME_POLL_TIMEOUT / FRAME_POLL_PERIOD; i++) {
      Long lastAccessed = getCachedImageLastAccessed(name, variant);
      
      if (lastAccessed == null) {
        try {
          lastAccessed = (Long)Uninterruptibles.getUninterruptibly(datastore.get(lastAccessedKey))
              .getProperty(VALUE_PROP);
        } catch (final ExecutionException e) {
          // Use 0 rather than Long.MIN_VALUE to ensure that we can subtract sanely.
          lastAccessed = 0L;
        }
      }
      
      // While this effectively throttles us at 1000 fps, the alternative of >= would immediately unblock the null case
      // unless we initialized that with a special lastAccessed = lastModified + 1.
      if (getImageLastModified(name,  variant) > lastAccessed) {
        break;
      }
      
      try {
        Thread.sleep(FRAME_POLL_PERIOD);
      } catch (final InterruptedException e) {
        log.warning(Throwables.getStackTraceAsString(e));
        break;
      }
    }
    
    long lastAccessed = System.currentTimeMillis();
    cache.put(name + "/" + variant + "/last-accessed", lastAccessed);
    final Entity lastAccessedEntity = new Entity(lastAccessedKey);
    lastAccessedEntity.setUnindexedProperty(VALUE_PROP, lastAccessed);
    datastore.put(lastAccessedEntity);
    
    return getImageBytes(name, variant);
  }
  
  public static void putAuditedImage(final String name, final String variant, final byte[] bytes) {
    putImageBytes(name, variant, bytes);
    
    final long lastModified = System.currentTimeMillis();
    cache.put(name + "/" + variant + "/last-modified", lastModified);
    
    final Entity lastModifiedEntity = new Entity(createKey(name, variant).getChild(METADATA_KIND, LAST_MODIFIED_KEY));
    lastModifiedEntity.setUnindexedProperty(VALUE_PROP, lastModified);
    datastore.put(lastModifiedEntity);
  }
  
  public static Future<Key> startPutEphemeral(final byte[] bytes) {
    final Entity ephemeral = new Entity(FRAME_KIND);
    ephemeral.setUnindexedProperty(BYTES_PROP, new Blob(bytes));
    return datastore.put(ephemeral);
  }
  
  public static long finishPutEphemeral(final Future<Key> key, final byte[] bytes) {
    long id;
    try {
      id = Uninterruptibles.getUninterruptibly(key).getId();
    } catch (final ExecutionException e) {
      log.warning(Throwables.getStackTraceAsString(e));
      id = 0;
    }
    cache.put(id, bytes);
    log.info("Ephemeral ID: " + id);
    return id;
  }
  
  public byte[] removeEphemeral(final long id) {
    final Key key = KeyFactory.createKey(FRAME_KIND, id);
    byte[] data = (byte[]) cache.get(id);
    cache.delete(id);
    if (data != null) {
      datastore.delete(key);
      return data;
    }
    
    try {
      data = ((Blob)Uninterruptibles.getUninterruptibly(datastore.get(key)).getProperty(BYTES_PROP)).getBytes();
      datastore.delete(key);
      return data;
    } catch (final ExecutionException e) {
      log.warning("No data for ID " + id);
      return null;
    }
  }
  
  private static Long getCachedImageLastAccessed(final String name, final String variant) {
    return readCachedLong(name + "/" + variant + "/last-accessed");
  }
  
  private static Long getCachedImageLastModified(final String name, final String variant) {
    return readCachedLong(name + "/" + variant + "/last-modified");
  }
  
  public static long getImageLastModified(final String name, final String variant) {
    Long ret = getCachedImageLastModified(name, variant);
    if (ret != null) {
      return ret;
    }
    
    try {
      ret = (Long)Uninterruptibles.getUninterruptibly(datastore.get(createKey(name, variant).getChild(METADATA_KIND, LAST_MODIFIED_KEY)))
          .getProperty(VALUE_PROP);
    } catch (final ExecutionException e) {
      ret = null; 
    }
    
    // Use 0 rather than Long.MIN_VALUE to ensure we can subtract sanely.
    return ret == null? 0 : ret;
  }
  
  private static @Nullable Long readCachedLong(final String key) {
    return (Long)cache.get(key);
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
