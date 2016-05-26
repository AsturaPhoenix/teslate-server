package io.baku.teslate;

import lombok.Getter;

@Getter
public class Command {
  private final long sequenceId;
  private final String commandString;
  public Command(final byte[] data) {
    commandString = new String(data);
    sequenceId = Long.parseLong(commandString.substring(0, commandString.indexOf('|')));
  }
}
