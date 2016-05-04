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
import com.google.appengine.tools.cloudstorage.GcsFileMetadata;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;

import lombok.extern.java.Log;

import com.google.appengine.api.images.ImagesServiceFactory;

@Log
public class Session {
  private static final String BUCKET = "simplecast-1297.appspot.com";
  private static final GcsFileOptions gcsFileOptions = new GcsFileOptions.Builder()
      .mimeType("image/jpeg")
      .build();
  private static final long
      STABLE_TIME = 4000,
      DIFF_THRESH = 128 * 128 * 256 * 256,
      STABLE_THRESH = 8 * 8 * 256 * 256;
  
  private static final BlobstoreService bs = BlobstoreServiceFactory.getBlobstoreService();
  private static final GcsService gcsService = GcsServiceFactory.createGcsService();
  private static final ImagesService imagesService = ImagesServiceFactory.getImagesService();
  
  private final BlobKey frameKey;
  private final GcsFilename
      frameFilename,
      stableFilename,
      prevFilename;
  private final ArrayList<Composite> composites = new ArrayList<>();
  
  private int[][] initialHistogram;
  
  private GcsFilename gcsFilename(final String variant) {
    return new GcsFilename(BUCKET, variant);
  }
  
  private BlobKey blobKey(final String variant) {
    return bs.createGsBlobKey("/gs/" + BUCKET + "/" + variant);
  }
  
  public Session() {
    frameKey = blobKey("frame.jpeg");
    frameFilename = gcsFilename("frame.jpeg");
    stableFilename = gcsFilename("stable.jpeg");
    prevFilename = gcsFilename("previous.jpeg");
  }
  
  public void refresh() throws IOException {
    composites.clear();
    if (gcsService.getMetadata(frameFilename) != null) {
      final Image original = ImagesServiceFactory.makeImageFromBlob(frameKey);
      initialHistogram = imagesService.histogram(original);
      composites.add(ImagesServiceFactory.makeComposite(original, 0, 0, 1, Composite.Anchor.TOP_LEFT));
    } else {
      initialHistogram = null;
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
    final Image result = composite();
    
    final boolean copyStable, copyPrevious;
    if (initialHistogram == null) {
      copyStable = copyPrevious = true;
    } else {
      long diff = diff(initialHistogram, imagesService.histogram(result));
      
      if (diff > DIFF_THRESH) {
        final GcsFileMetadata prevMd = gcsService.getMetadata(prevFilename);
        copyPrevious = prevMd == null || prevMd.getLastModified().getTime() < System.currentTimeMillis() - STABLE_TIME;
      } else {
        copyPrevious = false;
      }
      
      copyStable = diff < STABLE_THRESH;
    }

    if (copyStable) {
      gcsService.copy(frameFilename, stableFilename);
    }
    if (copyPrevious) {
      gcsService.copy(stableFilename, prevFilename);
    }
    
    gcsService.createOrReplace(frameFilename, gcsFileOptions,
        ByteBuffer.wrap(result.getImageData()));
  }
  
  public void get(final String variant, final HttpServletResponse resp) throws IOException {
    bs.serve(blobKey(variant), resp);
  }
  
  public long getLastModified(final String variant) throws IOException {
    return gcsService.getMetadata(gcsFilename(variant)).getLastModified().getTime();
  }
}
