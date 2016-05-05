package io.baku.simplecast;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.google.appengine.api.images.Composite;
import com.google.appengine.api.images.Image;
import com.google.appengine.api.images.ImagesService;
import com.google.appengine.api.images.ImagesService.OutputEncoding;

import lombok.extern.java.Log;

import com.google.appengine.api.images.ImagesServiceFactory;

@Log
public class Session {
  private static final String BUCKET = "simplecast-1297.appspot.com";
  private static final long
      STABLE_TIME = 4000,
      DIFF_THRESH = 192 * 192 * 256 * 256,
      STABLE_THRESH = 8 * 8 * 256 * 256;
  
  private static final BlobstoreService bs = BlobstoreServiceFactory.getBlobstoreService();
  private static final ImagesService imagesService = ImagesServiceFactory.getImagesService();
  
  private Image
      frameImage,
      stableImage,
      prevImage;
  private final ArrayList<Composite> composites = new ArrayList<>();
  private long lastScreen;
  
  private int[][] initialHistogram;
  
  public void refresh() throws IOException {
    composites.clear();
    if (frameImage != null) {
      composites.add(ImagesServiceFactory.makeComposite(frameImage, 0, 0, 1, Composite.Anchor.TOP_LEFT));
    }
  }
  
  private Image composite() {
    return imagesService.composite(composites, 72 * 6, 128 * 6, 0, OutputEncoding.JPEG);
  }
  
  public void update(int x, int y, byte[] bytes) {
    if (composites.size() == 15) {
      final Image step = composite();
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
        diff += dx * dx;
      }
    }
    
    log.info("diff: " + diff);
    
    return diff;
  }
  
  public void commit() throws IOException {
    frameImage = composite();
    
    final boolean copyStable, copyPrevious;
    final int[][] frameHistogram = imagesService.histogram(frameImage);
    if (initialHistogram == null) {
      copyStable = copyPrevious = true;
    } else {
      long diff = diff(initialHistogram, frameHistogram);
      
      if (diff > DIFF_THRESH) {
        copyPrevious =  lastScreen < System.currentTimeMillis() - STABLE_TIME;
      } else {
        copyPrevious = false;
      }
      
      copyStable = diff < STABLE_THRESH;
    }
    initialHistogram = frameHistogram;

    if (copyStable) {
      stableImage = frameImage;
    }
    if (copyPrevious) {
      prevImage = stableImage;
      lastScreen = System.currentTimeMillis();
    }
  }
  
  public void get(final String variant, final HttpServletResponse resp) throws IOException {
    resp.setContentType("image/jpeg");
    if (variant.equals("frame.jpeg")) {
      resp.getOutputStream().write(frameImage.getImageData());
    } else {
      resp.getOutputStream().write(prevImage.getImageData());
    }
  }
  
  public long getLastModified(final String variant) throws IOException {
    return lastScreen;
  }
}
