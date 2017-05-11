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

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.collect.Maps;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.Constants;
import zipkin.Endpoint;

import static brave.Span.Kind.CLIENT;
import static zipkin.internal.Util.checkNotNull;

/**
 * Creates traced sessions, which write directly to brave's collector to preserve the correct
 * duration.
 */
public final class TracingSession extends AbstractInvocationHandler implements LatencyTracker {
  private static final Logger LOG = LoggerFactory.getLogger(TracingSession.class);

  private final ProtocolVersion version;

  public static Session create(Session target, Tracing tracing) {
    return Reflection.newProxy(Session.class, new TracingSession(target, tracing));
  }

  final Session target;
  final Tracing tracing;
  final CurrentTraceContext currentTraceContext;
  final TraceContext.Injector<Map<String, ByteBuffer>> injector;
  /**
   * Manual Propagation, as opposed to attempting to control the {@link
   * Cluster#register(LatencyTracker) latency tracker callback thread}.
   */
  final Map<BoundStatement, Span> cache = Maps.newConcurrentMap();

  TracingSession(Session target, Tracing tracing) {
    this.target = checkNotNull(target, "target");
    this.tracing = checkNotNull(tracing, "tracing");
    this.currentTraceContext = tracing.currentTraceContext();
    this.injector = tracing.propagation().injector(new MapByteBufferSetter());
    this.version = target.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
    target.getCluster().register(this);
  }

  @Override protected Object handleInvocation(Object proxy, Method method, Object[] args)
      throws Throwable {
    Tracer tracer = tracing.tracer();
    // Only join traces, don't start them. This prevents LocalCollector's thread from amplifying.
    TraceContext parent = currentTraceContext.get();
    boolean shouldTrace = parent != null
        && method.getName().equals("executeAsync")
        && args[0] instanceof BoundStatement;
    if (!shouldTrace) {
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof RuntimeException) throw e.getCause();
        throw e;
      }
    }

    // via an internal class z.s.cassandra3.NamedBoundStatement, toString() is a nice name
    BoundStatement statement = (BoundStatement) args[0];
    Span span = tracer.newChild(parent).name(statement.toString()).kind(CLIENT);

    // o.a.c.tracing.Tracing.newSession must use the same format for the key zipkin
    if (version.compareTo(ProtocolVersion.V4) >= 0) {
      statement.enableTracing();
      Map<String, ByteBuffer> payload = new LinkedHashMap<>();
      injector.inject(span.context(), payload);
      statement.setOutgoingPayload(payload);
    }

    span.tag("cql.query", statement.preparedStatement().getQueryString());
    cache.put(statement, span.start());
    return new TracingResultSetFuture(target.executeAsync(statement), currentTraceContext, parent);
  }

  @Override public void update(Host host, Statement statement, Exception e, long nanos) {
    if (!(statement instanceof BoundStatement)) return;
    Span span = cache.remove(statement);
    if (span == null) {
      if (statement.isTracing()) {
        LOG.warn("{} not in the cache eventhough tracing is on", statement);
      }
      return;
    }

    Endpoint.Builder remoteEndpoint = Endpoint.builder().serviceName("cassandra");
    remoteEndpoint.parseIp(host.getAddress());
    remoteEndpoint.port(host.getSocketAddress().getPort());
    span.remoteEndpoint(remoteEndpoint.build());

    if (e != null) {
      String message = e.getMessage();
      span.tag(Constants.ERROR, message != null ? message : e.getClass().getSimpleName());
    }
    span.finish();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TracingSession) {
      TracingSession other = (TracingSession) obj;
      return target.equals(other.target);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return target.hashCode();
  }

  @Override
  public String toString() {
    return target.toString();
  }

  @Override public void onRegister(Cluster cluster) {
  }

  @Override public void onUnregister(Cluster cluster) {
  }
}
