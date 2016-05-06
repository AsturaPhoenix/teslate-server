package io.baku.simplecast;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheListener;
import javax.cache.CacheManager;
import javax.servlet.http.HttpServletResponse;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.appengine.api.images.Composite;
import com.google.appengine.api.images.Image;
import com.google.appengine.api.images.ImagesService;
import com.google.appengine.api.images.ImagesService.OutputEncoding;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import com.google.appengine.api.images.ImagesServiceFactory;
import com.google.appengine.api.images.ImagesServiceFailureException;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

@Log
@RequiredArgsConstructor
public class Session {
  private static final int
      SLEEP_PERIOD = 50,
      MAX_SLEEPS = 2000 / SLEEP_PERIOD;
  private static final long
      DIFF_MAGNITUDE = 10000;
  private static long
      SCREEN_REFRACTORY = 5000,
      STABLE_TIME = 1500,
      DIFF_THRESH = 55 * DIFF_MAGNITUDE;
  
  private static final ImagesService imagesService = ImagesServiceFactory.getImagesService();
  private static final Queue queue = QueueFactory.getDefaultQueue();
  
  private static final Cache cache;
  static {
    Cache c;
    try {
      c = CacheManager.getInstance().getCacheFactory().createCache(Collections.emptyMap());
    } catch (final CacheException e) {
      log.severe(Throwables.getStackTraceAsString(e));
      c = null;
    }
    cache = c;
  }
  
  private final String name;
  
  private final ArrayList<Composite> composites = new ArrayList<>();
  private int[][] initialHistogram;
  
  public void refresh() throws IOException {
    composites.clear();
    final byte[] frameBytes = (byte[])cache.get(name + "/frame.jpeg");
    if (frameBytes != null) {
      final Image frameImage = ImagesServiceFactory.makeImage(frameBytes);
      composites.add(ImagesServiceFactory.makeComposite(frameImage, 0, 0, 1, Composite.Anchor.TOP_LEFT));
      initialHistogram = histogramWithRetry(frameImage);
    } else {
      initialHistogram = null;
    }
  }
  
  private int[][] histogramWithRetry(final Image image) {
    for (int i = 0; i < 2; i++) {
      try {
        return imagesService.histogram(image);
      } catch (ImagesServiceFailureException e) {
        log.warning(Throwables.getStackTraceAsString(e));
      }
    }
    return imagesService.histogram(image);
  }
  
  private Image composite() {
    return imagesService.composite(composites, 72 * 6, 128 * 6, 0, OutputEncoding.JPEG);
  }
  
  private Image compositeWithRetry() {
    for (int i = 0; i < 2; i++) {
      try {
        return composite();
      } catch (ImagesServiceFailureException e) {
        log.warning(Throwables.getStackTraceAsString(e));
      }
    }
    return composite();
  }
  
  public void update(int x, int y, byte[] bytes) {
    if (composites.size() == 15) {
      final Image step = compositeWithRetry();
      composites.clear();
      composites.add(ImagesServiceFactory.makeComposite(step, 0, 0, 1, Composite.Anchor.TOP_LEFT));
    }
    
    final Image patch = ImagesServiceFactory.makeImage(bytes);
    composites.add(ImagesServiceFactory.makeComposite(patch, x, y, 1, Composite.Anchor.TOP_LEFT));
  }
  
  private long diff(int[][] a, int[][] b) {
    long diff = 0;
    for (int i = 0; i < a.length; i++) {
      for (int j = 0; j < a[i].length; j++) {
        int dx = a[i][j] - b[i][j];
        diff += Math.abs(dx);
      }
    }
    
    log.info("diff: " + diff / DIFF_MAGNITUDE);
    
    return diff;
  }
  
  public void config(final String[] param) {
    if ("sr".equals(param[2])) {
      SCREEN_REFRACTORY = Long.parseLong(param[3]);
    } else if ("st".equals(param[2])) {
      STABLE_TIME = Long.parseLong(param[3]);
    } else {
      DIFF_THRESH = Long.parseLong(param[3]);
    }
  }
  
  public void commit() throws IOException {
    final Image frameImage = compositeWithRetry();
    put("frame.jpeg", frameImage.getImageData());
    
    final boolean copyPrevious;
    final int[][] frameHistogram = histogramWithRetry(frameImage);
    if (initialHistogram == null) {
      copyPrevious = true;
    } else {
      long diff = diff(initialHistogram, frameHistogram);
      
      if (diff > DIFF_THRESH) {
        copyPrevious = System.currentTimeMillis() - getLastModified("previous.jpeg") > SCREEN_REFRACTORY;
      } else {
        copyPrevious = false;
      }
    }

    final String stableKey = "/frame/" + name + "/stable.jpeg";
    queue.add(TaskOptions.Builder.withUrl(stableKey)
        .etaMillis(System.currentTimeMillis() + STABLE_TIME)
        .method(Method.PUT)
        .payload(frameImage.getImageData()));
    
    if (copyPrevious) {
      put("previous.jpeg", (byte[])cache.get(name + "/stable.jpeg"));
    }
  }
  
  @SuppressWarnings("unchecked")
  public void put(final String variant, final byte[] payload) {
    final String key = name + "/" + variant;
    cache.put(key, payload);
    cache.put(key + "/last-modified", System.currentTimeMillis());
  }
  
  @SuppressWarnings("unchecked")
  public void get(final String variant, final HttpServletResponse resp) throws IOException {
    final String key = name + "/" + variant;
    for (int i = 0; i < MAX_SLEEPS && readLong(key + "/last-accessed") > readLong(key + "/last-modified"); i++) {
      try {
        Thread.sleep(SLEEP_PERIOD);
      } catch (final InterruptedException e) {
        log.warning(Throwables.getStackTraceAsString(e));
      }
    }
    
    cache.put(key + "/last-accessed", System.currentTimeMillis());
    
    final byte[] content = (byte[])cache.get(key);
    if (content == null) {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    } else {
      resp.setContentType("image/jpeg");
      try (final OutputStream o = resp.getOutputStream()) {
    	  o.write(content);
      }
    }
  }
  
  private long readLong(final String key) {
    final Long value = (Long)cache.get(key);
    return value == null? Long.MIN_VALUE : value;
  }
  
  public long getLastModified(final String variant) throws IOException {
    return readLong(name + "/" + variant + "/last-modified");
  }
}
