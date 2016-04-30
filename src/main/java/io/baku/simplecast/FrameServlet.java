/*
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.baku.simplecast;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.io.ByteStreams;

import lombok.AllArgsConstructor;
import lombok.Synchronized;
import lombok.extern.java.Log;

@Log
public class FrameServlet extends HttpServlet {
  private static final long serialVersionUID = -1428114883956980006L;
  
  @AllArgsConstructor
  private static class Frame {
    public final String mimeType;
    public final byte[] bytes;
    public final long lastModified;
  }
  
  private final Map<String, Frame> frames = new HashMap<>();

  @Synchronized
  @Override
  public void doPut(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
    frames.put(req.getPathInfo(), new Frame(
        req.getContentType(),
        ByteStreams.toByteArray(req.getInputStream()),
        System.currentTimeMillis()));
    log.info("Wrote to frame " + req.getPathInfo());
  }
  
  @Override
  protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    final Frame frame = frames.get(req.getPathInfo());
    if (frame ==  null) {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    } else {
      resp.setContentType(frame.mimeType);
      resp.setDateHeader("Last-Modified", frame.lastModified);
    }
  }
  
  @Synchronized
  @Override
  public void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
    final Frame frame = frames.get(req.getPathInfo());
    log.info("Read from frame " + req.getPathInfo());
    if (frame ==  null) {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    } else {
      resp.setContentType(frame.mimeType);
      resp.setDateHeader("Last-Modified", frame.lastModified);
      try (final OutputStream o = resp.getOutputStream()) {
        o.write(frame.bytes);
      }
    }
  }
}
