package io.baku.simplecast;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.images.Composite;
import com.google.appengine.api.images.Image;
import com.google.appengine.api.images.ImagesService;
import com.google.appengine.api.images.ImagesService.OutputEncoding;
import com.google.appengine.api.images.ImagesServiceFactory;
import com.google.appengine.api.images.ImagesServiceFailureException;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

@Log
@RequiredArgsConstructor
public class Session {
  private static final long
      DIFF_MAGNITUDE = 10000;
  private static long
      SCREEN_REFRACTORY = 5000,
      STABLE_UPDATE = 2000,
      STABLE_TIME = 2000,
      DIFF_THRESH = 70 * DIFF_MAGNITUDE;
  
  private static final ImagesService imagesService = ImagesServiceFactory.getImagesService();
  private static final Queue queue = QueueFactory.getDefaultQueue();
  
  private final String name;
  
  private final Object compositeMutex = new Object();
  private final ArrayList<Composite> composites = new ArrayList<>();
  private int[][] initialHistogram;
  private int width, height;
  private int patchArea; 
  
  private boolean validateDimensions(final int w, final int h) {
    return w <= 4000 && w > 0 && h <= 4000 && h > 0;
  }
  
  public void refresh() throws IOException {
    final byte[] frameBytes = Persistence.getImageBytes(name, "frame.jpeg");
    synchronized (compositeMutex) {
      composites.clear();
      if (frameBytes != null) {
        final Image frameImage = ImagesServiceFactory.makeImage(frameBytes);
        width = frameImage.getWidth();
        height = frameImage.getHeight();
        if (validateDimensions(width, height)) {
          composites.add(ImagesServiceFactory.makeComposite(frameImage, 0, 0, 1, Composite.Anchor.TOP_LEFT));
          initialHistogram = histogramWithRetry(frameImage);
        } else {
          log.warning("Previous frame is corrupt: " + width + " x " + height);
          width = height = 0;
          initialHistogram = null;
        }
      } else {
        width = height = 0;
        initialHistogram = null;
      }
      patchArea = 0;
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
    return imagesService.composite(composites, width, height, 0, OutputEncoding.PNG);
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
  
  private Image convert(final Image source) {
    return imagesService.applyTransform(
        ImagesServiceFactory.makeResize(
            source.getWidth(), source.getHeight()),
        source,
        OutputEncoding.JPEG);
  }
  
  private Image convertWithRetry(final Image source) {

    for (int i = 0; i < 2; i++) {
      try {
        return convert(source);
      } catch (ImagesServiceFailureException e) {
        log.warning(Throwables.getStackTraceAsString(e));
      }
    }
    return convert(source);
  }
  
  public void update(int x, int y, byte[] bytes) {
    final Image patch = ImagesServiceFactory.makeImage(bytes);
    final int
      cwidth = Math.max(width, x + patch.getWidth()),
      cheight = Math.max(height, y + patch.getHeight());
    
    if (validateDimensions(cwidth, cheight)) {
      synchronized (compositeMutex) {
        width = cwidth;
        height = cheight;
        
        patchArea += patch.getWidth() * patch.getHeight();
        
        if (composites.size() == 15) {
          final Image step = compositeWithRetry();
          composites.clear();
          composites.add(ImagesServiceFactory.makeComposite(step, 0, 0, 1, Composite.Anchor.TOP_LEFT));
        }
        composites.add(ImagesServiceFactory.makeComposite(patch, x, y, 1, Composite.Anchor.TOP_LEFT));
      }
    } else {
      log.warning("Invalid patch " + patch.getWidth() + " x " + patch.getHeight() +
          " at " + x + ", " + y);
    }
  }
  
  private static long diff(int[][] a, int[][] b) {
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
      DIFF_THRESH = Long.parseLong(param[3]) * DIFF_MAGNITUDE;
    }
  }
  
  public void commit() throws IOException {
    final Image frameImage;
    synchronized (compositeMutex) {
       frameImage = compositeWithRetry();
    }
    
    final Long frameLastPersistedDurably = Persistence.getCachedImageLastPersistedDurably(name, "frame.jpeg");
    
    if (frameLastPersistedDurably == null || System.currentTimeMillis() >= frameLastPersistedDurably + STABLE_UPDATE) {
      final UUID frameUuid = Persistence.saveImage(name, frameImage.getImageData());
      Persistence.setRef(name, "frame.jpeg", frameUuid, true);

      final String stableKey = "/frame/" + name + "/stable.jpeg";
      queue.add(TaskOptions.Builder.withUrl(stableKey)
          .etaMillis(System.currentTimeMillis() + STABLE_TIME)
          .method(Method.PUT)
          .payload(Persistence.uuidToBytes(frameUuid)));
    } else {
      Persistence.setDirect(name, "frame.jpeg", frameImage.getImageData());
    }
    
    final boolean copyPrevious;
    if (initialHistogram == null || patchArea == 0 || patchArea < width * height / 4) {
      copyPrevious = false;
      log.info("Not diffing; patch area " + patchArea * 100 / width / height + "%");
    } else {
      final int[][] frameHistogram = histogramWithRetry(frameImage);
      
      final long diff;
      final int[][] initialHistogram;
      synchronized (compositeMutex) {
        initialHistogram = this.initialHistogram;
      }
      
      diff = diff(initialHistogram, frameHistogram);
      
      if (diff > DIFF_THRESH) {
        copyPrevious = System.currentTimeMillis() - getLastModified("previous.jpeg") > SCREEN_REFRACTORY;
      } else {
        copyPrevious = false;
      }
    }

    if (copyPrevious) {
      try {
        final UUID stableUuid = Persistence.getRef(name, "stable.jpeg");
        log.info("Copying previous");
        Persistence.setRef(name, "previous.jpeg", stableUuid, true);
      } catch (final EntityNotFoundException e) {
        log.info("No stable.jpeg reference found");
      }
    }
  }
  
  public void put(final String variant, final UUID uuid) throws IOException {
    Persistence.setRef(name, variant, uuid, false);
  }
  
  public void handleDatastoreTask(final String variant, final ObjectInputStream oin)
      throws ClassNotFoundException, IOException {
    Persistence.handleDatastoreTask(name, variant, oin);
  }
  
  public void get(final String variant, final HttpServletResponse resp) throws IOException {
    final byte[] content = Persistence.awaitAuditedImageBytes(name, variant);
    if (content == null) {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    } else {
      resp.setContentType("image/jpeg");
      
      Image contentImage = ImagesServiceFactory.makeImage(content);
      if (contentImage.getFormat() != Image.Format.JPEG) {
        contentImage = convertWithRetry(contentImage);
      }
      
      try (final OutputStream o = resp.getOutputStream()) {
    	  o.write(contentImage.getImageData());
      }
    }
  }
  
  public long getLastModified(final String variant) throws IOException {
    return Persistence.getImageLastModified(name,  variant);
  }
}
