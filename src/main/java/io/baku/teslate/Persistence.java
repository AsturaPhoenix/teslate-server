package io.baku.teslate;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;

import javax.annotation.Nullable;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.appengine.api.datastore.AsyncDatastoreService;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.memcache.ConsistentLogAndContinueErrorHandler;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheService.IdentifiableValue;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import lombok.extern.java.Log;

@Log
@UtilityClass
public class Persistence {
  private static final int
    REF_STICKINESS = 10000,
    USE_POLL_PERIOD = 2000,
    FRAME_POLL_PERIOD = 50,
    FRAME_POLL_TIMEOUT = 5000,
    COMMAND_POLL_PERIOD = 50,
    COMMAND_POLL_TIMEOUT = 10000;
  
  private static final String
    SESSION_KIND = "Session",
    FRAME_KIND = "Frame",
    FRAME_REF_KIND = "FrameRef",
    BYTES_PROP = "bytes",
    TIMESTAMP_PROP = "timestamp",
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
    return KeyFactory.createKey(SESSION_KIND, name).getChild(FRAME_REF_KIND, variant);
  }
  
  private static Key createKey(final UUID uuid) {
    return KeyFactory.createKey(FRAME_KIND, uuid.toString());
  }
  
  public static byte[] getImageBytes(final UUID uuid) {
    final byte[] cached = (byte[])cache.get(uuid);
    if (cached != null) {
      return cached;
    }
    
    log.info("Cache miss (" + uuid + ")");
    
    try {
      return ((Blob)Uninterruptibles.getUninterruptibly(datastore.get(createKey(uuid)))
          .getProperty(BYTES_PROP)).getBytes();
    } catch (final ExecutionException e) {
      log.warning("No frame for " + uuid + " in datastore.");
      return null;
    }
  }
  
  public static byte[] uuidToBytes(final UUID uuid) {
    return ByteBuffer.allocate(2 * Long.BYTES)
        .putLong(uuid.getMostSignificantBits())
        .putLong(uuid.getLeastSignificantBits())
        .array();
  }
  
  public static UUID bytesToUuid(final byte[] bytes) {
    final ByteBuffer buff = ByteBuffer.wrap(bytes);
    return new UUID(buff.getLong(), buff.getLong());
  }
  
  public static byte[] getImageBytes(final String name, final String variant) {
    final Object cached = cache.get(name + "/" + variant);
    if (cached instanceof byte[]) {
      log.info("Direct image retrieved from " + name + "/" + variant);
      return (byte[]) cached;
    } else if (cached instanceof UUID) {
      log.info("Image ref at " + name + "/" + variant + ": " + cached);
      return getImageBytes((UUID)cached);
    }
    
    log.info("Cache miss (" + name + "/" + variant + ")");
    
    final UUID uuid;
    try {
      uuid = getDatastoreRef(name, variant);
    } catch (final EntityNotFoundException e) {
      log.warning("Session " + name + ": no frame for " + variant + " in datastore.");
      return null;
    }
    return getImageBytes(uuid);
  }
  
  public static UUID getRef(final String name, final String variant) throws EntityNotFoundException {
    final UUID ref = (UUID)cache.get(name + "/" + variant);
    if (ref != null) {
      return ref;
    }
    
    log.info("Cache miss (" + name + "/" + variant + ")");
    return getDatastoreRef(name, variant);
  }
  
  private static UUID getDatastoreRef(final String name, final String variant) throws EntityNotFoundException {
    final Entity raw = finishGet(datastore.get(createKey(name, variant)));
    return propToUuid(raw.getProperty(BYTES_PROP));
  }
  
  private static <T> T finishGet(final Future<T> future) throws EntityNotFoundException {
    try {
      return Uninterruptibles.getUninterruptibly(future);
    } catch (final ExecutionException e) {
      throw (EntityNotFoundException)e.getCause();
    }
  }
  
  private static UUID propToUuid(final Object prop) {
    return bytesToUuid(((Blob)prop).getBytes());
  }
  
  @RequiredArgsConstructor
  private static class SetDatastoreRefTask implements Serializable {
    private static final long serialVersionUID = 6753130426063123693L;
    
    public final UUID ref;
    public final @Nullable Long lastModified;
  }
  
  public static void setDirect(final String name, final String variant, final byte[] data) {
    final String nvKey = name + "/" + variant;
    cache.put(nvKey, data);
    log.info("Setting image for " + nvKey + " directly");
    cache.put(nvKey + "/last-modified", System.currentTimeMillis());
  }
  
  public static void setRef(final String name, final String variant, final UUID ref,
      final boolean frontEnd) throws IOException {
    final String nvKey = name + "/" + variant;
    cache.put(nvKey, ref);
    log.info("Changing " + nvKey + " ref to " + ref + " in cache");
    
    if (frontEnd) {
      final long lastModified = System.currentTimeMillis();
      cache.put(nvKey + "/last-modified", lastModified);
      cache.put(nvKey + "/last-persisted-durably", lastModified);
      scheduleDatastoreTask(name, variant, new SetDatastoreRefTask(ref, lastModified));
    } else {
      setDatastoreRef(name, variant, new SetDatastoreRefTask(ref, null));
    }
  }
  
  private static void setDatastoreRef(final String name, final String variant,
      final SetDatastoreRefTask task) {
    
    final Key frameKey = createKey(name, variant);
    UUID prev = null;

    boolean committed = false;
    while (!committed) {
      final Transaction tx = Futures.getUnchecked(datastore.beginTransaction());
      try {
        try {
          prev = propToUuid(finishGet(datastore.get(tx, frameKey)).getProperty(BYTES_PROP));
          log.info("Previous ref for " + frameKey + ": " + prev);
        } catch (final EntityNotFoundException e) {
          prev = null;
        }
        
        final Entity frameRef = new Entity(frameKey);
        frameRef.setUnindexedProperty(BYTES_PROP, new Blob(uuidToBytes(task.ref)));
        Futures.getUnchecked(datastore.put(tx, frameRef));
        
        if (task.lastModified != null) {
          final Entity lastModifiedEntity = new Entity(frameKey
              .getChild(METADATA_KIND, LAST_MODIFIED_KEY));
          lastModifiedEntity.setUnindexedProperty(VALUE_PROP, task.lastModified);
          Futures.getUnchecked(datastore.put(tx, lastModifiedEntity));
        }
        
        tx.commit();
        committed = true;
      } catch (final ConcurrentModificationException e) {
        log.info("Retrying due to concurrent modification: " + Throwables.getStackTraceAsString(e));
      } finally {
        if (tx.isActive()) {
          tx.rollback();
        }
      }
    }
    
    log.info("Changed " + variant + " ref to " + task.ref + " in datastore");
      
    if (prev != null && !isInUse(name, prev)) {
      // evict
      cache.delete(prev);
      datastore.delete(createKey(prev));
      log.info(prev + " evicted from cache and datastore");
    }
  }
  
  private void scheduleDatastoreTask(final String name, final String variant, final Serializable task) {
    Async.trap(Async.EXEC.submit(() -> handleDatastoreTask(name, variant, task)));
  }
  
  private void scheduleDatastoreTask(final String name, final Serializable task) {
    Async.trap(Async.EXEC.submit(() -> handleDatastoreTask(name, task)));
  }
  
  private static void handleDatastoreTask(final String name, final String variant, final Object task) {
    if (task instanceof SetDatastoreRefTask) {
      setDatastoreRef(name, variant, (SetDatastoreRefTask)task);
    } else if (task instanceof SetDatastoreLastAccessedTask) {
      setDatastoreLastAccessed(name, variant, (SetDatastoreLastAccessedTask)task);
    } else if (task instanceof SaveDatastoreImageTask) {
      saveDatastoreImage(name, (SaveDatastoreImageTask)task);
    }
  }
  
  private static void handleDatastoreTask(final String name, final Object task) {
    saveDatastoreImage(name, (SaveDatastoreImageTask)task);
  }
  
  @RequiredArgsConstructor
  private static class SetDatastoreLastAccessedTask implements Serializable {
    private static final long serialVersionUID = -5200407633524247853L;
    
    public final long lastAccessed;
  }
  
  private static Key createLastAccessedKey(final String name, final String variant) {
    return createKey(name, variant).getChild(METADATA_KIND, LAST_ACCESSED_KEY);
  }
  
  public static byte[] awaitAuditedImageBytes(final String name, final String variant)
      throws IOException {
    // Use 0 rather than Long.MIN_VALUE to ensure that we can subtract sanely.
    final long lastAccessed;
    final Long cachedLastAccessed = getCachedImageLastAccessed(name, variant);
    
    if (cachedLastAccessed == null) {
      log.info("Cache miss (" + name + "/" + variant + "/last-accessed)");
      lastAccessed = 0;
    } else {
      lastAccessed = cachedLastAccessed;
    }
    
    byte[] imageBytes = null;
    //Poll for changes because memcache doesn't expose distributed events
    while (true) {
      if (System.currentTimeMillis() >= lastAccessed + FRAME_POLL_TIMEOUT ||
          getImageLastModified(name,  variant) > lastAccessed) {
      	imageBytes = getAuditedImageBytes(name, variant);
      	if (imageBytes != null) {
      	  break;
      	}
      }
      
      try {
        Thread.sleep(FRAME_POLL_PERIOD);
      } catch (final InterruptedException e) {
        log.warning(Throwables.getStackTraceAsString(e));
        break;
      }
    }
    
    return imageBytes == null ? getAuditedImageBytes(name, variant) : imageBytes;
  }
  
  private static byte[] getAuditedImageBytes(final String name, final String variant) {
    cache.put(name + "/" + variant + "/last-accessed", System.currentTimeMillis());
    return getImageBytes(name, variant);
  }
  
  private static void setDatastoreLastAccessed(final String name, final String variant,
      final SetDatastoreLastAccessedTask task) {
    final Entity lastAccessedEntity = new Entity(createLastAccessedKey(name, variant));
    lastAccessedEntity.setUnindexedProperty(VALUE_PROP, task.lastAccessed);
    datastore.put(lastAccessedEntity);
  }
  
  @RequiredArgsConstructor
  private static class SaveDatastoreImageTask implements Serializable {
    private static final long serialVersionUID = 1551510793636566075L;
    public final UUID uuid;
    public final byte[] bytes;
    public final long timestamp;
  }
  
  public static UUID saveImage(final String name, final byte[] bytes) throws IOException {
    final UUID uuid = UUID.randomUUID();
    cache.put(uuid, bytes);
    log.info(uuid + " saved to cache");
    
    scheduleDatastoreTask(name, new SaveDatastoreImageTask(uuid, bytes, System.currentTimeMillis()));
    
    return uuid;
  }
  
  private static void saveDatastoreImage(final String name, final SaveDatastoreImageTask task) {
    final Entity entity = new Entity(createKey(task.uuid));
    entity.setUnindexedProperty(BYTES_PROP, new Blob(task.bytes));
    entity.setUnindexedProperty(TIMESTAMP_PROP, task.timestamp);
    datastore.put(entity);
    
    log.info(task.uuid + " saved to datastore");

    try {
      Thread.sleep(REF_STICKINESS);
    } catch (final InterruptedException e) {
      log.warning(Throwables.getStackTraceAsString(e));
    }
    
    evictIfUnused(name, task.uuid);
  }
  
  private static void evictIfUnused(final String name, final UUID frame) {
    if (isInUse(name, frame)) {
      log.info(frame + " not evicted due to ref");
    } else {
      cache.delete(frame);
      datastore.delete(createKey(frame));
      log.info(frame + " evicted from cache and datastore");
    }
  }
  
  private static boolean isInUse(final String name, final UUID frame) {
    if (checkUse(name, frame)) {
      return true;
    }
    
    try {
      Thread.sleep(USE_POLL_PERIOD);
    } catch (final InterruptedException e) {
      log.warning(Throwables.getStackTraceAsString(e));
    }
    
    return checkUse(name, frame);
  }
  
  private static boolean checkUse(final String name, final UUID frame) {
    UUID ref;
    try {
      ref = getRef(name, "previous.jpeg"); 
    } catch (final EntityNotFoundException e) {
      ref = null;
    }
    
    if (frame.equals(ref)) {
      return true;
    }
    
    try {
      ref = getRef(name, "stable.jpeg");
    } catch (final EntityNotFoundException e) {
      return false;
    }
    
    return frame.equals(ref);
  }
  
  private static Long getCachedImageLastAccessed(final String name, final String variant) {
    return readCachedLong(name + "/" + variant + "/last-accessed");
  }
  
  private static Long getCachedImageLastModified(final String name, final String variant) {
    return readCachedLong(name + "/" + variant + "/last-modified");
  }
  
  public static Long getCachedImageLastPersistedDurably(final String name, final String variant) {
    return readCachedLong(name + "/" + variant + "/last-persisted-durably");
  }
  
  public static long getImageLastModified(final String name, final String variant) {
    Long ret = getCachedImageLastModified(name, variant);
    if (ret != null) {
      return ret;
    }
    
    log.info("Cache miss (" + name + "/" + variant + "/last-modified)");
    
    try {
      ret = (Long)finishGet(datastore.get(createKey(name, variant)
          .getChild(METADATA_KIND, LAST_MODIFIED_KEY)))
          .getProperty(VALUE_PROP);
    } catch (final EntityNotFoundException e) {
      ret = null;
    }
    
    // Use 0 rather than Long.MIN_VALUE to ensure we can subtract sanely.
    return ret == null? 0 : ret;
  }
  
  public static long awaitImageLastModified(final String name, final String variant) {
    final String lastQueriedKey = name + "/" + variant + "/last-queried";
    // Use 0 rather than Long.MIN_VALUE to ensure that we can subtract sanely.
    final long lastQueried;
    long lastModified = 0;
    Long cachedLastQueried = readCachedLong(lastQueriedKey);
    
    if (cachedLastQueried == null) {
      log.info("Cache miss (" + lastQueriedKey + ")");
      lastQueried = 0;
    } else {
      lastQueried = cachedLastQueried;
    }
    
    //Poll for changes because memcache doesn't expose distributed events
    while(true) {
      lastModified = getImageLastModified(name,  variant);
      if (lastModified > lastQueried ||
          System.currentTimeMillis() >= lastQueried + FRAME_POLL_TIMEOUT) {
        break;
      }
      
      try {
        Thread.sleep(FRAME_POLL_PERIOD);
      } catch (final InterruptedException e) {
        log.warning(Throwables.getStackTraceAsString(e));
        break;
      }
    }
    
    cache.put(lastQueriedKey, System.currentTimeMillis());
    
    return lastModified;
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
