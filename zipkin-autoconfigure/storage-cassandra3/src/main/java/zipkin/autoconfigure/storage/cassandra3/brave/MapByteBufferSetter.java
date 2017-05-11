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

import brave.propagation.Propagation;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.util.Map;

import static zipkin.internal.Util.UTF_8;

final class MapByteBufferSetter implements Propagation.Setter<Map<String, ByteBuffer>, String> {
  static final ThreadLocal<CharsetEncoder> UTF8_ENCODER = new ThreadLocal<CharsetEncoder>() {
    @Override protected CharsetEncoder initialValue() {
      return UTF_8.newEncoder();
    }
  };

  @Override public void put(Map<String, ByteBuffer> carrier, String key, String value) {
    try {
      carrier.put(key, UTF8_ENCODER.get().encode(CharBuffer.wrap(value)));
    } catch (CharacterCodingException e1) {
    }
  }
}