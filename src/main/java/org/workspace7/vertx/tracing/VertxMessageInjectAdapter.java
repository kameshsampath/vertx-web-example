package org.workspace7.vertx.tracing;

import io.opentracing.propagation.TextMap;
import io.vertx.core.json.JsonObject;

import java.util.Iterator;
import java.util.Map;

/**
 * @author kameshs
 */
public class VertxMessageInjectAdapter implements TextMap {

    private JsonObject header;

    public VertxMessageInjectAdapter(final JsonObject obj) {
        header = obj.getJsonObject("_apmHeader");
        if (header == null) {
            header = new JsonObject();
            obj.put("_apmHeader", header);
        }
    }

    public static void cleanup(JsonObject obj) {
        obj.remove("_apmHeader");
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        throw new UnsupportedOperationException("VertxMessageInjectAdapter should only be used with Tracer.inject()");
    }

    @Override
    public void put(String key, String value) {
        this.header.put(key, value);
    }
}
