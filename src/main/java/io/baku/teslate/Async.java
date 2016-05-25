package io.baku.teslate;

import java.util.concurrent.Executors;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import lombok.experimental.UtilityClass;
import lombok.extern.java.Log;

@UtilityClass
@Log
public class Async {
  public static final ListeningScheduledExecutorService EXEC =
      MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));
  
  public static void trap(final ListenableFuture<?> future) {
    Futures.addCallback(future, new FutureCallback<Object>() {
      @Override
      public void onSuccess(final Object result) {
      }
      
      @Override
      public void onFailure(final Throwable t) {
        log.warning(Throwables.getStackTraceAsString(t));
      }
    });
  }
}
