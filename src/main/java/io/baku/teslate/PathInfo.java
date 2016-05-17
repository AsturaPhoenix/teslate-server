package io.baku.teslate;

import javax.servlet.http.HttpServletRequest;

public class PathInfo {
  public final String name;
  public final String variant;
  
  public PathInfo(final HttpServletRequest req) {
    final String[] parts = req.getPathInfo().split("/");
    name = parts[1];
    variant = parts.length > 2 ? parts[2] : null;
  }
}