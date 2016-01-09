package com.quorum;

public class ElectionException extends Exception {
    public ElectionException(String message, Object... args) {
        super(String.format(message, args));
    }
}
