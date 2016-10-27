package org.workspace7.vertx.tracing;

import io.opentracing.propagation.TextMap;
import io.vertx.core.MultiMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author kameshs
 */
public class HttpHeadersExtractAdapter implements TextMap {

    private final Map<String, String> map;

    public HttpHeadersExtractAdapter(final MultiMap multiMap) {
        this.map = new HashMap<>();
        multiMap.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void put(String s, String s1) {
        throw new UnsupportedOperationException("TextMapInjectAdapter should only be used with Tracer.extract()");
    }
}
