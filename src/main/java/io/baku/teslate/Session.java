package io.baku.teslate;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletResponse;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.appengine.api.datastore.EntityNotFoundException;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

@Log
@RequiredArgsConstructor
public class Session {
  private static long
      SCREEN_REFRACTORY = 5000,
      STABLE_UPDATE = 2000,
      STABLE_TIME = 2000;
  
  private static final long
      COMMAND_TIMEOUT = 10000;
  
  private static final int
      DIFF_COLOR_THRESH = 128,
      DIFF_RES = 5;
  
  private static final float
      DIFF_THRESH = .22f;
  
  private final String name;
  
  private final Object compositeMutex = new Object();
  private BufferedImage frame, composite;
  private volatile BufferedImage stable;
  private final Object prevMutex = new Object();
  private byte[] previous;
  private long lastScreen, lastPersistedDurably;
  private final Semaphore
      frameSemaphore = new Semaphore(0),
      prevSemaphore = new Semaphore(0);
  private Graphics2D g;
  private int patchArea;
  
  private boolean validateDimensions(final int w, final int h) {
    return w <= 4000 && w > 0 && h <= 4000 && h > 0;
  }
  
  public void setDims(int w, int h) {
    synchronized (compositeMutex) {
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
    synchronized (compositeMutex) {
      if (frame == null) {
        final byte[] frameBytes = Persistence.getImageBytes(name, "frame.jpeg");
        if (frameBytes != null) {
          frame = read(frameBytes);
          
          if (!validateDimensions(frame.getWidth(), frame.getHeight())) {
            log.warning("Previous frame is corrupt: " + frame.getWidth() + " x " + frame.getHeight());
            frame = null;
          }
        } else {
          frame = null;
        }
      }
      if (frame != null) {
        composite(0, 0, frame);
      }
      patchArea = 0;
    }
  }
  
  private void composite(final int x, final int y, final BufferedImage patch) {
    g.drawImage(patch, x, y, null);
  }
  
  public void update(int x, int y, byte[] bytes) {
    final BufferedImage patch = read(bytes);
    final int
      cwidth = x + patch.getWidth(),
      cheight = y + patch.getHeight();
    
    if (cwidth > 0 && cwidth <= composite.getWidth() && cheight > 0 && cheight <= composite.getHeight()) {
      synchronized (compositeMutex) {
        patchArea += patch.getWidth() * patch.getHeight();
        composite(x, y, patch);
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
        log.info("diff: " + acc * 100f / denom + "%");
        return true;
    } else if (acc > pxThreshold / 2) {
        log.info("diff: " + acc * 100f / denom + "% (below threshold)");
    }

    return false;
}
  
  public void commit() throws IOException {
    if (g != null) {
      g.dispose();
      g = null;
    }

    final BufferedImage initialFrame, frame;
    final int patchArea;
    synchronized(compositeMutex) {
      initialFrame = this.frame;
      frame = this.frame = this.composite;
      frameSemaphore.release();
      patchArea = this.patchArea;
    }
    
    Async.trap(Async.EXEC.submit(() -> {
      final byte[] compositeBytes = write(frame, "png");
      
      if (System.currentTimeMillis() >= lastPersistedDurably + STABLE_UPDATE) {
        final UUID frameUuid = Persistence.saveImage(name, compositeBytes);
        Persistence.setRef(name, "frame.jpeg", frameUuid, true);
        log.info("Persisting " + frameUuid);
        
        Async.trap(Async.EXEC.schedule(() -> {
          try {
            stable = frame;
            Persistence.setRef(name, "stable.jpeg", frameUuid, false);
          } catch (final Exception e) {
            log.warning(Throwables.getStackTraceAsString(e));
          }
        }, STABLE_TIME, TimeUnit.MILLISECONDS));
      }
      
      final boolean copyPrevious;
      if (initialFrame == null || patchArea == 0) {
        copyPrevious = false;
      } else if (patchArea < frame.getWidth() * frame.getHeight() / 4) {
        copyPrevious = false;
        log.info("Not diffing; patch area " + patchArea * 100 / frame.getWidth() / frame.getHeight() + "%");
      } else {
        
        if (diff(initialFrame, frame)) {
          copyPrevious = System.currentTimeMillis() - getLastModified("previous.jpeg") > SCREEN_REFRACTORY;
        } else {
          copyPrevious = false;
        }
      }
  
      if (copyPrevious) {
        synchronized (prevMutex) {
          previous = write(stable, "jpeg");
          lastScreen = System.currentTimeMillis();
          prevSemaphore.release();
        }
        
        try {
          final UUID stableUuid = Persistence.getRef(name, "stable.jpeg");
          log.info("Copying previous");
          Persistence.setRef(name, "previous.jpeg", stableUuid, true);
        } catch (final EntityNotFoundException e) {
          log.info("No stable.jpeg reference found");
        }
      }
      return null;
    }));
  }
  
  public void put(final String variant, final UUID uuid) throws IOException {
    Persistence.setRef(name, variant, uuid, false);
  }
  
  public void get(final String variant, final HttpServletResponse resp) throws IOException {
    if ("frame.jpeg".equals(variant)) {
      try {
        frameSemaphore.tryAcquire(Persistence.FRAME_POLL_TIMEOUT, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        log.warning(Throwables.getStackTraceAsString(e));
      }
      
      synchronized(compositeMutex) {
        frameSemaphore.drainPermits();
        if (frame == null) {
          resp.sendError(HttpServletResponse.SC_NOT_FOUND);
        } else {
          resp.setContentType("image/jpeg");
          try (final OutputStream o = resp.getOutputStream()) {
            o.write(write(frame, "jpeg"));
          }
        }
      }
    } else if ("previous.jpeg".equals(variant)) {
      // we've already waited in the HEAD call
      synchronized(prevMutex) {
        prevSemaphore.drainPermits();
        if (previous == null) {
          resp.sendError(HttpServletResponse.SC_NOT_FOUND);
        } else {
          resp.setContentType("image/jpeg");
          try (final OutputStream o = resp.getOutputStream()) {
            o.write(previous);
          }
        }
      }
    }
  }
  
  public long getLastModified(final String variant) throws IOException {
    return Persistence.getImageLastModified(name,  variant);
  }
  
  public long awaitLastModified(final String variant) throws IOException {
    try {
      prevSemaphore.tryAcquire(Persistence.FRAME_POLL_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      log.warning(Throwables.getStackTraceAsString(e));
    }
    
    synchronized(prevMutex) {
      prevSemaphore.drainPermits();
      return lastScreen;
    }
  }
  
  private final ArrayList<Command> commands = new ArrayList<>();
  
  public void pushCommand(final byte[] data) {
    final Command command = new Command(data);
    synchronized(commands) {
      int i;
      for (i = commands.size(); i > 0; i--) {
        if (commands.get(i - 1).getSequenceId() <= command.getSequenceId()) {
          break;
        }
      }
      commands.add(i, command);
      commands.notifyAll();
    }
  }
  
  public byte[] flushCommands() {
    synchronized(commands) {
      if (commands.isEmpty()) {
        try {
          commands.wait(COMMAND_TIMEOUT);
        } catch (final InterruptedException e) {
          log.warning(Throwables.getStackTraceAsString(e));
        }
      }
      
      final String serialized = commands.stream()
        .map(Command::getCommandString)
        .collect(Collectors.joining("\n"));
      commands.clear();
      return serialized.getBytes();
    }
  }
}
