package io.baku.simplecast;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
import com.google.appengine.api.memcache.ConsistentLogAndContinueErrorHandler;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheService.IdentifiableValue;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

import lombok.RequiredArgsConstructor;
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
    FRAME_REF_KIND = "FrameRef",
    BYTES_PROP = "bytes",
    METADATA_KIND = "Metadata",
    REF_COUNT_KEY = "refCount",
    LAST_ACCESSED_KEY = "lastAccessed",
    LAST_MODIFIED_KEY = "lastModified",
    VALUE_PROP = "value";
  
  private static final int BACKEND_FRAME_REFS = 1;
  
  private static final Queue queue = QueueFactory.getDefaultQueue();
  
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
    final UUID uuid;
    try {
      uuid = getRef(name, variant);
    } catch (final EntityNotFoundException e) {
      log.warning("Session " + name + ": no frame for " + variant + " in datastore.");
      return null;
    }
    return getImageBytes(uuid);
  }
  
  public static UUID getRef(final String name, final String variant) throws EntityNotFoundException {
    return finishGetRef(startGetRef(name, variant));
  }
  
  private static Future<?> startGetRef(final String name, final String variant) {
    UUID uuid = (UUID)cache.get(name + "/" + variant);
    return uuid == null? datastore.get(createKey(name, variant)) : Futures.immediateFuture(uuid);
  }
  
  private static <T> T finishGet(final Future<T> future) throws EntityNotFoundException {
    try {
      return Uninterruptibles.getUninterruptibly(future);
    } catch (final ExecutionException e) {
      throw (EntityNotFoundException)e.getCause();
    }
  }
  
  private static UUID finishGetRef(final Future<?> future) throws EntityNotFoundException {
    final Object result = finishGet(future);
    return result instanceof UUID? (UUID)result :
      bytesToUuid(((Blob)((Entity)result).getProperty(BYTES_PROP)).getBytes());
  }
  
  @RequiredArgsConstructor
  private static class SetDatastoreRefTask implements Serializable {
    private static final long serialVersionUID = 6753130426063123693L;
    
    public final UUID prevUuid, newUuid;
    public final int prevRefCount;
    public final @Nullable Integer newRefCount;
    public final @Nullable Long lastModified;
  }
  
  public static void setRef(final String name, final String variant, final UUID uuid,
      final boolean frontEnd) throws IOException {
    final Future<?> prevFuture;
    prevFuture = startGetRef(name, variant);
    
    final Integer newRc;
    if (frontEnd) {
      // increment new refcount
      final Integer rcToIncrement = getRefCount(uuid);
      newRc = rcToIncrement == null? 1 : rcToIncrement + 1;
      cache.put(uuid + "/refcount", newRc);
      log.info(uuid + " refcount " + newRc + " in cache");
    } else {
      // back-end frames have their refcount prepopulated
      newRc = null;
    }

    // flip ref
    cache.put(name + "/" + variant, uuid);
    log.info("Changing ref to " + uuid + " in cache");
    
    // decrement old refcount or evict
    UUID prev;
    try {
      prev = finishGetRef(prevFuture);
    } catch (final EntityNotFoundException e) {
      prev = null;
    }
    
    int prevRc = -1;
    
    if (prev != null) {
      final Integer rcToDecrement = getRefCount(prev);
      
      if (rcToDecrement == null || rcToDecrement <= 1) {
        // evict
        cache.deleteAll(Arrays.asList(prev, prev + "/refcount"));
        log.info(prev + " evicted from cache");
        prevRc = 0;
      } else {
        // decrement
        prevRc = rcToDecrement - 1;
        cache.put(prev + "/refcount", prevRc);
        log.info(prev + " refcount " + prevRc + " in cache");
      }
    }
    
    if (frontEnd) {
      final long lastModified = System.currentTimeMillis();
      cache.put(name + "/" + variant + "/last-modified", lastModified);
      scheduleDatastoreTask(name, variant,
          new SetDatastoreRefTask(prev, uuid, prevRc, newRc, lastModified));
    } else {
      setDatastoreRef(name, variant,
          new SetDatastoreRefTask(prev, uuid, prevRc, null, null));
    }
  }
  
  private void scheduleDatastoreTask(final String datastoreTaskUrl, final Serializable task) 
      throws IOException {
    final ByteArrayOutputStream payload = new ByteArrayOutputStream();
    try (final ObjectOutputStream oout = new ObjectOutputStream(payload)) {
      oout.writeObject(task);
    }
    queue.add(TaskOptions.Builder.withUrl(datastoreTaskUrl)
        .method(Method.POST)
        .payload(payload.toByteArray()));
  }
  
  private void scheduleDatastoreTask(final String name, final String variant, final Serializable task) 
      throws IOException {
    scheduleDatastoreTask("/frame/" + name + "/" + variant, task);
  }
  
  private void scheduleDatastoreTask(final Serializable task) throws IOException {
    scheduleDatastoreTask("/frame", task);
  }
  
  public static void handleDatastoreTask(final String name, final String variant,
      final ObjectInputStream oin) throws ClassNotFoundException, IOException {
    final Object task = oin.readObject();
    
    if (task instanceof SetDatastoreRefTask) {
      setDatastoreRef(name, variant, (SetDatastoreRefTask)task);
    } else if (task instanceof SetDatastoreLastAccessedTask) {
      setDatastoreLastAccessed(name, variant, (SetDatastoreLastAccessedTask)task);
    } else if (task instanceof SaveDatastoreImageTask) {
      saveDatastoreImage((SaveDatastoreImageTask)task);
    }
  }
  
  public static void handleDatastoreTask(final ObjectInputStream oin)
      throws ClassNotFoundException, IOException {
    final Object task = oin.readObject();
    saveDatastoreImage((SaveDatastoreImageTask)task);
  }
  
  private static void setDatastoreRef(final String name, final String variant,
      final SetDatastoreRefTask task) {
    if (task.newUuid != null) {
      final Entity newRcEntity = new Entity(createRefCountKey(task.newUuid));
      newRcEntity.setUnindexedProperty(VALUE_PROP, task.newRefCount);
      datastore.put(newRcEntity);
      log.info(task.newUuid + " refcount " + task.newRefCount + " in datastore");
    }
    
    final Key frameKey = createKey(name, variant);
    final Entity frame = new Entity(frameKey);
    frame.setUnindexedProperty(BYTES_PROP, new Blob(uuidToBytes(task.newUuid)));
    datastore.put(frame);
    log.info("Changing ref from " + task.prevUuid + " to " + task.newUuid + " in datastore");
    
    if (task.prevUuid != null) {
      final Key prevKey = createRefCountKey(task.prevUuid);
      if (task.prevRefCount <= 0) {
        // evict
        datastore.delete(prevKey.getParent(), prevKey);
        log.info(task.prevUuid + " evicted from datastore");
      } else {
        // decrement
        final Entity oldRcEntity = new Entity(prevKey);
        oldRcEntity.setUnindexedProperty(VALUE_PROP, task.prevRefCount);
        datastore.put(oldRcEntity);
        log.info(task.prevUuid + " refcount " + task.prevRefCount + " in datastore");
      }
    }
    
    if (task.lastModified != null) {
      final Entity lastModifiedEntity = new Entity(frameKey
          .getChild(METADATA_KIND, LAST_MODIFIED_KEY));
      lastModifiedEntity.setUnindexedProperty(VALUE_PROP, task.lastModified);
      datastore.put(lastModifiedEntity);
    }
  }
  
  private static Key createRefCountKey(final UUID uuid) {
    return createKey(uuid).getChild(METADATA_KIND, REF_COUNT_KEY);
  }
  
  private static Integer getRefCount(final UUID uuid) {
    final Key key = createRefCountKey(uuid);
    Integer rc = (Integer)cache.get(uuid + "/refcount");
    if (rc == null) {
      try {
        // this is stupid
        rc = (int)(long)(Long)finishGet(datastore.get(key)).getProperty(VALUE_PROP);
      } catch (final EntityNotFoundException e) {
        rc = null;
      }
    }
    return rc;
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
    //Poll for changes because memcache doesn't expose distributed events
    final Key lastAccessedKey = createLastAccessedKey(name, variant);
    
    for (int i = 0; i < FRAME_POLL_TIMEOUT / FRAME_POLL_PERIOD; i++) {
      Long lastAccessed = getCachedImageLastAccessed(name, variant);
      
      if (lastAccessed == null) {
        try {
          lastAccessed = (Long)finishGet(datastore.get(lastAccessedKey)).getProperty(VALUE_PROP);
        } catch (final EntityNotFoundException e) {
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
    
    scheduleDatastoreTask(name, variant, new SetDatastoreLastAccessedTask(lastAccessed));
    
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
  }
  
  public static UUID saveImage(final byte[] bytes) throws IOException {
    final UUID uuid = UUID.randomUUID();
    cache.put(uuid, bytes);
    cache.put(uuid + "/refcount", BACKEND_FRAME_REFS);
    log.info(uuid + " saved to cache (initial refs: " + BACKEND_FRAME_REFS + ")");
    
    scheduleDatastoreTask(new SaveDatastoreImageTask(uuid));
    
    return uuid;
  }
  
  private static void saveDatastoreImage(final SaveDatastoreImageTask task) {
    final byte[] bytes = (byte[])cache.get(task.uuid);
    if (bytes == null) {
      log.warning("Could not persist " + task.uuid + " due to cache miss");
    } else {
      final Key rcKey = createRefCountKey(task.uuid);
      final Entity entity = new Entity(rcKey.getParent());
      entity.setUnindexedProperty(BYTES_PROP, new Blob(bytes));
      datastore.put(entity);
      
      final Entity rcEntity = new Entity(rcKey);
      rcEntity.setUnindexedProperty(VALUE_PROP, BACKEND_FRAME_REFS);
      datastore.put(rcEntity);
      
      log.info(task.uuid + " saved to datastore (initial refs: " + BACKEND_FRAME_REFS + ")");
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
      ret = (Long)finishGet(datastore.get(createKey(name, variant)
          .getChild(METADATA_KIND, LAST_MODIFIED_KEY)))
          .getProperty(VALUE_PROP);
    } catch (final EntityNotFoundException e) {
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
