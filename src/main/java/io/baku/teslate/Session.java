package io.baku.teslate;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.UUID;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletResponse;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.java.Log;

@Log
@RequiredArgsConstructor
public class Session {
  private static long
      SCREEN_REFRACTORY = 5000,
      STABLE_UPDATE = 2000,
      STABLE_TIME = 2000;
  
  private static final int
      DIFF_COLOR_THRESH = 128,
      DIFF_RES = 5;
  
  private static final float
      DIFF_THRESH = .22f;
  
  private static final Queue queue = QueueFactory.getDefaultQueue();
  
  private final String name;
  
  private final Object compositeMutex = new Object();
  private BufferedImage initialFrame, composite;
  private Graphics2D g;
  private int patchArea;
  
  private static class Timer {
    private long t0, ti, tl, dt;
    
    public void start() {
      tl = System.currentTimeMillis();
      if (ti < 0) {
        ti = tl - t0;
      }
    }
    
    public void stop() {
      dt += System.currentTimeMillis() - tl;
    }
    
    public void reset() {
      dt = 0;
      ti = -1;
      t0 = System.currentTimeMillis();
    }
    
    @Override
    public String toString() {
      return "(dt: " + Long.toString(dt) + " ms, ti: " + Long.toString(ti) + " ms)";
    }
  }
  
  @ToString
  private static class Metrics {
    public final Timer
      prevDecode = new Timer(),
      prevComposite = new Timer(),
      patchDecode = new Timer(),
      patchComposite = new Timer();
    private int patchCount;
    private final Timer
      compositeEncode = new Timer();
    
    public void reset() {
      prevDecode.reset();
      prevComposite.reset();
      patchDecode.reset();
      patchComposite.reset();
      patchCount = 0;
      compositeEncode.reset();
    }
  }
  
  private final Metrics metrics = new Metrics();
  
  private boolean validateDimensions(final int w, final int h) {
    return w <= 4000 && w > 0 && h <= 4000 && h > 0;
  }
  
  public void setDims(int w, int h) {
    synchronized (compositeMutex) {
      metrics.reset();
      
      composite = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
      if (g != null) {
        g.dispose();
      }
      g = composite.createGraphics();
    }
  }
  
  private BufferedImage read(final byte[] bytes) {
    try {
      return ImageIO.read(new ByteArrayInputStream(bytes));
    } catch (final IOException e) {
      log.severe(Throwables.getStackTraceAsString(e));
      throw new RuntimeException(e);
    }
  }
  
  private byte[] write(final BufferedImage img, final String format) {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try {
      ImageIO.write(img, format, bout);
    } catch (final IOException e) {
      log.severe(Throwables.getStackTraceAsString(e));
      throw new RuntimeException(e);
    }
    return bout.toByteArray();
  }
  
  public void refresh() {
    final byte[] frameBytes = Persistence.getImageBytes(name, "frame.jpeg");
    synchronized (compositeMutex) {
      if (frameBytes != null) {
        metrics.prevDecode.start();
        initialFrame = read(frameBytes);
        metrics.prevDecode.stop();
        
        if (validateDimensions(initialFrame.getWidth(), initialFrame.getHeight())) {
          metrics.prevComposite.start();
          composite(0, 0, initialFrame);
          metrics.prevComposite.stop();
        } else {
          log.warning("Previous frame is corrupt: " + initialFrame.getWidth() + " x " + initialFrame.getHeight());
          initialFrame = null;
        }
      } else {
        initialFrame = null;
      }
      patchArea = 0;
    }
  }
  
  private void composite(final int x, final int y, final BufferedImage patch) {
    g.drawImage(patch, x, y, null);
  }
  
  public void update(int x, int y, byte[] bytes) {
    metrics.patchDecode.start();
    final BufferedImage patch = read(bytes);
    metrics.patchDecode.stop();
    final int
      cwidth = x + patch.getWidth(),
      cheight = y + patch.getHeight();
    
    if (cwidth > 0 && cwidth <= composite.getWidth() && cheight > 0 && cheight <= composite.getHeight()) {
      synchronized (compositeMutex) {
        patchArea += patch.getWidth() * patch.getHeight();
        
        metrics.patchComposite.start();
        composite(x, y, patch);
        metrics.patchComposite.stop();
        
        metrics.patchCount++;
      }
    } else {
      log.warning("Invalid patch " + patch.getWidth() + " x " + patch.getHeight() +
          " at " + x + ", " + y);
    }
  }
  
  public void config(final String[] param) {
    if ("sr".equals(param[2])) {
      SCREEN_REFRACTORY = Long.parseLong(param[3]);
    } else if ("st".equals(param[2])) {
      STABLE_TIME = Long.parseLong(param[3]);
    }
  }
  
  private static boolean diff(final Color a, final Color b) {
    return Math.abs(a.getRed() - b.getRed()) > DIFF_COLOR_THRESH ||
           Math.abs(a.getGreen() - b.getGreen()) > DIFF_COLOR_THRESH ||
           Math.abs(a.getBlue() - b.getBlue()) > DIFF_COLOR_THRESH;
  }
  
  private boolean diff(final BufferedImage a, final BufferedImage b) {
    final int denom = a .getHeight() / DIFF_RES * a.getWidth() / DIFF_RES;
    final int pxThreshold = (int)(DIFF_THRESH * denom);

    int acc = 0;
    for (int y = 0; y < a.getHeight(); y += DIFF_RES) {
        for (int x = 0; x < a.getWidth(); x += DIFF_RES) {
            if (diff(new Color(a.getRGB(x, y)), new Color(b.getRGB(x, y)))) {
                acc++;
            }
        }
    }
    if (acc > pxThreshold) {
        System.out.println("diff: " + acc * 100f / denom + "%");
        return true;
    } else if (acc > pxThreshold / 2) {
        System.out.println("diff: " + acc * 100f / denom + "% (below threshold)");
    }

    return false;
}
  
  public void commit() throws IOException {
    if (g != null) {
      g.dispose();
      g = null;
    }
    
    final Long frameLastPersistedDurably = Persistence.getCachedImageLastPersistedDurably(name, "frame.jpeg");
    
    metrics.compositeEncode.start();
    final byte[] compositeBytes = write(composite, "png");
    metrics.compositeEncode.stop();
    
    if (frameLastPersistedDurably == null || System.currentTimeMillis() >= frameLastPersistedDurably + STABLE_UPDATE) {
      final UUID frameUuid = Persistence.saveImage(name, compositeBytes);
      Persistence.setRef(name, "frame.jpeg", frameUuid, true);

      final String stableKey = "/frame/" + name + "/stable.jpeg";
      queue.add(TaskOptions.Builder.withUrl(stableKey)
          .etaMillis(System.currentTimeMillis() + STABLE_TIME)
          .method(Method.PUT)
          .payload(Persistence.uuidToBytes(frameUuid)));
    } else {
      Persistence.setDirect(name, "frame.jpeg", compositeBytes);
    }
    
    final boolean copyPrevious;
    if (initialFrame == null || patchArea == 0) {
      copyPrevious = false;
    } else if (patchArea < composite.getWidth() * composite.getHeight() / 4) {
      copyPrevious = false;
      log.info("Not diffing; patch area " + patchArea * 100 / composite.getWidth() / composite.getHeight() + "%");
    } else {
      final BufferedImage initialFrame, composite;
      synchronized (compositeMutex) {
        initialFrame = this.initialFrame;
        composite = this.composite;
      }
      
      if (diff(initialFrame, composite)) {
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
    
    log.info(metrics.toString());
  }
  
  public void put(final String variant, final UUID uuid) throws IOException {
    Persistence.setRef(name, variant, uuid, false);
  }
  
  public void handleDatastoreTask(final String variant, final ObjectInputStream oin)
      throws ClassNotFoundException, IOException {
    Persistence.handleDatastoreTask(name, variant, oin);
  }
  
  public void get(final String variant, final HttpServletResponse resp) throws IOException {
    byte[] content = Persistence.awaitAuditedImageBytes(name, variant);
    
    if (content == null) {
      content = Persistence.getImageBytes(name, "stable.jpeg");
    }
    
    if (content == null) {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    } else {
      resp.setContentType("image/jpeg");
      
      final BufferedImage contentImage = read(content);
      
      try (final OutputStream o = resp.getOutputStream()) {
    	  ImageIO.write(contentImage, "jpeg", o);
      }
    }
  }
  
  public long getLastModified(final String variant) throws IOException {
    return Persistence.getImageLastModified(name,  variant);
  }
  
  public long awaitLastModified(final String variant) throws IOException {
    return Persistence.awaitImageLastModified(name, variant);
  }
}
