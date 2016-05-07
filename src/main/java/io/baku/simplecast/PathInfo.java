package io.baku.simplecast;

import javax.servlet.http.HttpServletRequest;

public class PathInfo {
  public final String name;
  public final String variant;
  
  public PathInfo(final HttpServletRequest req) {
    final String[] parts = req.getPathInfo().split("/");
    name = parts[1];
    variant = parts[2];
  }
}