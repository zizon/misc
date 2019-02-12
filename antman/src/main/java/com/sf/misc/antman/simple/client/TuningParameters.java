package com.sf.misc.antman.simple.client;

public interface TuningParameters {

    default long chunk() {
        return 64 * 1024;
    }

    default long netIOBytesPerSecond() {
        return 100 * 1024;
    }

    default long diskIOBytesPerSecond() {
        return 10 * 1024 * 1024;
    }

    default long scheduleCostPerChunk() {
        return 10;
    }

    default long driftDelay() {
        return 1;
    }
}
