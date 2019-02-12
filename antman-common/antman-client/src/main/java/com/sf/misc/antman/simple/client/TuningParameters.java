package com.sf.misc.antman.simple.client;

public interface TuningParameters {

    public static TuningParameters DEFAULT = new TuningParameters() {
    };

    default long chunk() {
        return 4 * 1024 * 1024;
    }

    default long ackTimeout() {
        return ((chunk() / (1024 * 100L))) * 1000L + 1;// 100 kbps
    }


}
