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

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.Synchronized;
import lombok.extern.java.Log;

@Log
public class FrameServlet extends HttpServlet {
  private static final long serialVersionUID = -1428114883956980006L;
  
  private final Map<String, Session> sessions = new HashMap<>();

  @Synchronized
  @Override
  public void doPut(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
    Session s = sessions.get("nautilus");
    if (s == null) {
      s = new Session();
      sessions.put("nautilus", s);
    }
    
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
    
    log.info("Wrote to frame " + req.getPathInfo());
  }
  
  @Synchronized
  @Override
  protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    final String[] pathInfo = req.getPathInfo().split("/");
    final Session s = sessions.get("nautilus");
    
    if (s ==  null) {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    } else {
      resp.setContentType("image/jpeg");
      resp.setDateHeader("Last-Modified", s.getLastModified(pathInfo[1]));
    }
  }
  
  @Synchronized
  @Override
  public void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
    final String[] pathInfo = req.getPathInfo().split("/");
    final Session s = sessions.get("nautilus");
    
    if (s ==  null) {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    } else {
      s.get(pathInfo[1], resp);
    }
    
    log.info("Read from frame " + req.getPathInfo());
  }
}
