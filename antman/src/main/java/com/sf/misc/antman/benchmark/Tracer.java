package com.sf.misc.antman.benchmark;

import java.util.concurrent.TimeUnit;

public class Tracer {

    protected final String name;

    protected long mark;
    protected long accumulate;

    public Tracer(String name) {
        this.name = name;
        this.accumulate = 0;
        this.mark = System.nanoTime();
    }

    public Tracer reset() {
        this.accumulate = 0;
        this.mark = 0;
        return this;
    }

    public Tracer start() {
        this.mark = System.nanoTime();
        return this;
    }

    public Tracer suspend() {
        long end = System.nanoTime();
        this.accumulate += end - mark;
        return this;
    }

    public Tracer resume() {
        this.mark = System.nanoTime();
        return this;
    }

    public Tracer stop() {
        return this.suspend();
    }

    public long cost() {
        return this.accumulate;
    }

    public String name() {
        return this.name;
    }

    @Override
    public String toString() {
        return "Tracer:" + this.name() + " cost:" + TimeUnit.NANOSECONDS.toMillis(this.accumulate) + " ms";
    }
}
