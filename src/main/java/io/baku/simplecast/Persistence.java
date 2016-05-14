package io.baku.simplecast;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
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
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.memcache.ConsistentLogAndContinueErrorHandler;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheService.IdentifiableValue;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.google.common.util.concurrent.Uninterruptibles;

import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import lombok.extern.java.Log;

@Log
@UtilityClass
public class Persistence {
  private static final int
    REF_STICKINESS = 7000,
    FRAME_POLL_PERIOD = 50,
    FRAME_POLL_TIMEOUT = 2500,
    COMMAND_POLL_PERIOD = 50,
    COMMAND_POLL_TIMEOUT = 10000;
  
  private static final String
    SESSION_KIND = "Session",
    FRAME_KIND = "Frame",
    FRAME_REF_KIND = "FrameRef",
    REF_COUNTER_KIND = "RefCounter",
    BYTES_PROP = "bytes",
    REF_COUNTER_PROP = "refCounter",
    METADATA_KIND = "Metadata",
    LAST_ACCESSED_KEY = "lastAccessed",
    LAST_MODIFIED_KEY = "lastModified",
    VALUE_PROP = "value";
  
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
    final UUID uuid;
    try {
      uuid = getRef(name, variant);
    } catch (final EntityNotFoundException e) {
      log.warning("Session " + name + ": no frame for " + variant + " in datastore.");
      return null;
    }
    return getImageBytes(uuid);
  }
  
  @RequiredArgsConstructor
  public static class RefEntity implements Serializable {
    private static final long serialVersionUID = -7865697301362093803L;
    public final UUID ref;
    public final UUID refCounter;
    
    @Override
    public String toString() {
      return ref + " (ref instance " + refCounter + ")";
    }
  }
  
  public static UUID getRef(final String name, final String variant) throws EntityNotFoundException {
    final RefEntity ref = (RefEntity)cache.get(name + "/" + variant);
    if (ref != null) {
      return ref.ref;
    }
    
    log.info("Cache miss (" + name + "/" + variant + ")");
    
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
    
    public final RefEntity prevRef, newRef;
    public final boolean incrementNew;
    public final @Nullable Long lastModified;
  }
  
  public static void setRef(final String name, final String variant, final RefEntity refEntity,
      final boolean frontEnd) throws IOException {
    final String nvKey = name + "/" + variant;
    RefEntity prev;
    final IdentifiableValue prevFromCache = cache.getIdentifiable(nvKey);
    if (prevFromCache == null) {
      log.info("Cache miss (" + nvKey + ")");
      
      final Future<Entity> prevFromDatastore = datastore.get(createKey(name, variant));
      cache.put(nvKey, refEntity);
      try {
        final Entity raw = finishGet(prevFromDatastore);
        prev = new RefEntity(propToUuid(raw.getProperty(BYTES_PROP)),
            propToUuid(raw.getProperty(REF_COUNTER_PROP)));
      } catch (final EntityNotFoundException e) {
        prev = null;
      }
    } else {
      if (!cache.putIfUntouched(nvKey, prevFromCache, refEntity)) {
        setRef(name, variant, refEntity, frontEnd);
        return;
      }
      prev = (RefEntity)prevFromCache.getValue();
    }
    log.info("Changing " + variant + " ref from " + prev + " to " + refEntity + " in cache");
    
    if (frontEnd) {
      final long lastModified = System.currentTimeMillis();
      cache.put(name + "/" + variant + "/last-modified", lastModified);
      scheduleDatastoreTask(name, variant,
          new SetDatastoreRefTask(prev, refEntity, true, lastModified));
    } else {
      setDatastoreRef(name, variant,
          new SetDatastoreRefTask(prev, refEntity, false, null));
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
  
  private static Key createRefCounterKey(final RefEntity refEntity) {
    return createKey(refEntity.ref).getChild(REF_COUNTER_KIND, refEntity.refCounter.toString());
  }
  
  private static void setDatastoreRef(final String name, final String variant,
      final SetDatastoreRefTask task) {
    if (task.incrementNew) {
      final Entity newRcEntity = new Entity(createRefCounterKey(task.newRef));
      datastore.put(newRcEntity);
      log.info(task.newRef.ref + " refcount incremented in datastore (counter " + task.newRef.refCounter + ")");
    }
    
    final Key frameKey = createKey(name, variant);
    final Entity frameRef = new Entity(frameKey);
    frameRef.setUnindexedProperty(BYTES_PROP, new Blob(uuidToBytes(task.newRef.ref)));
    frameRef.setUnindexedProperty(REF_COUNTER_PROP, new Blob(uuidToBytes(task.newRef.refCounter)));
    datastore.put(frameRef);
    log.info("Changing " + variant + " ref from " +
        task.prevRef + " to " + task.newRef + " in datastore");
    
    if (task.lastModified != null) {
      final Entity lastModifiedEntity = new Entity(frameKey
          .getChild(METADATA_KIND, LAST_MODIFIED_KEY));
      lastModifiedEntity.setUnindexedProperty(VALUE_PROP, task.lastModified);
      datastore.put(lastModifiedEntity);
    }
    
    if (task.prevRef != null) {
      try {
        // There's a chance the ref is in the middle of being passed around. To deal with this without locking,
        // hold the ref for a time. This also covers the edge case where the increment is severely delayed.
        Thread.sleep(REF_STICKINESS);
      } catch (final InterruptedException e) {
        log.warning(Throwables.getStackTraceAsString(e));
      }
      
      final Key prevCounterKey = createRefCounterKey(task.prevRef);
      // decrement
      try {
        datastore.delete(prevCounterKey).get();
        log.info(task.prevRef.ref + " refcount decremented in datastore (counter " + task.prevRef.refCounter + ")");
      } catch (InterruptedException | ExecutionException e) {
        log.warning(Throwables.getStackTraceAsString(e));
      }
      
      if (!datastore.prepare(new Query(REF_COUNTER_KIND)
          .setAncestor(prevCounterKey.getParent())
          .setKeysOnly())
          .asIterator().hasNext()) {
        // evict
        cache.delete(task.prevRef.ref);
        datastore.delete(prevCounterKey.getParent());
        log.info(task.prevRef.ref + " evicted from cache and datastore");
      }
    }
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
        log.info("Cache miss (" + name + "/" + variant + "/last-accessed)");
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
    log.info(uuid + " saved to cache");
    
    scheduleDatastoreTask(new SaveDatastoreImageTask(uuid));
    
    return uuid;
  }
  
  private static void saveDatastoreImage(final SaveDatastoreImageTask task) {
    final byte[] bytes = (byte[])cache.get(task.uuid);
    if (bytes == null) {
      log.warning("Could not persist " + task.uuid + " due to cache miss");
    } else {
      // Since we only have one back-end frame ref to prepopulate, the stable ref (which will be
      // hard-reffed about 5 seconds after persistence), just use the image's UUID for that counter.
      final Key rcKey = createRefCounterKey(new RefEntity(task.uuid, task.uuid));
      datastore.put(new Entity(rcKey));
      
      final Entity entity = new Entity(rcKey.getParent());
      entity.setUnindexedProperty(BYTES_PROP, new Blob(bytes));
      datastore.put(entity);
      
      log.info(task.uuid + " saved to datastore (initial counter: " + task.uuid + ")");
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
