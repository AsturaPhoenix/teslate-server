package io.baku.teslate;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.io.ByteStreams;

public class CommandServlet extends HttpServlet {
  private static final long serialVersionUID = -1059865976923447315L;

  @Override
  protected void doPost(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    Sessions.getOrCreate(new PathInfo(req).name).pushCommand(ByteStreams.toByteArray(req.getInputStream()));
  }

  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    resp.setContentType("text/plain");
    final byte[] command = Sessions.getOrCreate(new PathInfo(req).name).flushCommands();
    if (command == null) {
      resp.sendError(HttpServletResponse.SC_NO_CONTENT);
    } else {
      try (final OutputStream o = resp.getOutputStream()) {
        o.write(command);
      }
    }
  }
}
