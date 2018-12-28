package com.sf.misc.antman.buffers;

public class Exceptions {

    public static class BufferException extends Exception {
        public BufferException(String message) {
            super(message);
        }
    }

    public static class DuplicatedPageProcessorException extends BufferException {
        public DuplicatedPageProcessorException(String message) {
            super(message);
        }
    }
}
