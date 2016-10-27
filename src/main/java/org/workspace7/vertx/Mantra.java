package org.workspace7.vertx;

import io.vertx.core.json.JsonObject;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kameshs
 */
public class Mantra {

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private final int id;

    private String mantra;

    public Mantra(String mantra) {
        this.id = COUNTER.getAndIncrement();
        this.mantra = mantra;
    }

    public Mantra(int id, String mantra) {
        this.id = id;
        this.mantra = mantra;
    }

    public Mantra() {
        this.id = -1;
    }

    public Mantra(JsonObject json) {
        this.id = json.getInteger("ID");
        this.mantra = json.getString("MANTRA");
    }

    public int getId() {
        return id;
    }

    public String getMantra() {
        return mantra;
    }

    public void setMantra(String mantra) {
        this.mantra = mantra;
    }
}
