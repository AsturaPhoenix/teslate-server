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

package io.baku.teslate;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.io.ByteStreams;

public class FrameServlet extends HttpServlet {
  private static final long serialVersionUID = -1428114883956980006L;
  
  @Override
  protected void doPut(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
    final PathInfo p = new PathInfo(req);
    final Session s = Sessions.getOrCreate(p.name);
    
    if ("frame.jpeg".equals(p.variant)) {
      s.refresh();
      
      try (final ObjectInputStream oi = new ObjectInputStream(req.getInputStream())) {
        while (true) {
          final int x = oi.readInt(), y = oi.readInt();
          final byte[] buff = new byte[oi.readInt()];
          oi.readFully(buff, 0, buff.length);
          s.update(x, y, buff);
        }
      } catch (final EOFException ignore) {
        s.commit();
      }
    } else {
      s.put(p.variant, Persistence.bytesToUuid(ByteStreams.toByteArray(req.getInputStream())));
    }
  }
  
  @Override
  protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
    final PathInfo p = new PathInfo(req);
    
    try (final ObjectInputStream oin = new ObjectInputStream(req.getInputStream())) {
      if (req.getPathInfo().length() < 3) {
        Persistence.handleDatastoreTask(p.name, oin);
      } else {
        Sessions.getOrCreate(p.name).handleDatastoreTask(p.variant, oin);
      }
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    final PathInfo p = new PathInfo(req);
    final Session s = Sessions.get(p.name);
    
    if (s ==  null) {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    } else {
      resp.setContentType("image/jpeg");
      resp.setDateHeader("Last-Modified", s.getLastModified(p.variant));
    }
  }
  
  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
    final PathInfo p = new PathInfo(req);
    final Session s = Sessions.get(p.name);
    
    if (s ==  null) {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    } else {
      s.get(p.variant, resp);
    }
  }
}
