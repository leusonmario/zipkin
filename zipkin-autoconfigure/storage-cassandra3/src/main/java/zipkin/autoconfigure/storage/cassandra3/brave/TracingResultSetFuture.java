/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.autoconfigure.storage.cassandra3.brave;

import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class TracingResultSetFuture<T> implements ResultSetFuture {
  final ResultSetFuture delegate;
  final CurrentTraceContext currentTraceContext;
  final TraceContext parent;

  TracingResultSetFuture(ResultSetFuture delegate, CurrentTraceContext currentTraceContext,
      TraceContext parent) {
    this.delegate = delegate;
    this.currentTraceContext = currentTraceContext;
    this.parent = parent;
  }

  @Override public ResultSet getUninterruptibly() {
    return delegate.getUninterruptibly();
  }

  @Override public ResultSet getUninterruptibly(long timeout, TimeUnit unit)
      throws TimeoutException {
    return delegate.getUninterruptibly(timeout, unit);
  }

  @Override public boolean cancel(boolean mayInterruptIfRunning) {
    return delegate.cancel(mayInterruptIfRunning);
  }

  @Override public boolean isCancelled() {
    return delegate.isCancelled();
  }

  @Override public boolean isDone() {
    return delegate.isDone();
  }

  @Override public ResultSet get() throws InterruptedException, ExecutionException {
    return delegate.get();
  }

  @Override public ResultSet get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return delegate.get(timeout, unit);
  }

  @Override public void addListener(Runnable listener, Executor executor) {
    delegate.addListener(() -> {
      try (CurrentTraceContext.Scope scope = currentTraceContext.newScope(parent)) {
        listener.run();
      }
    }, executor);
  }
}