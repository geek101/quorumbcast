package com.quorum.util;


import org.slf4j.Logger;

public class LogPrefix {
    private final Logger log;
    private String prefix;

    public LogPrefix(Logger log, String prefix) {
        this.log = log;
        this.prefix = prefix;
    }

    public void debug(String message) {
        log.debug(this.prefix + ": " + message);
    }

    public void info(String message) {
        log.info(this.prefix + ": " + message);
    }

    public void error(String message) {
        log.error(this.prefix + ": " + message);
    }

    public void error(String message, Throwable t) {
        log.error(this.prefix + ": " + message, t);
    }

    public void warn(String message) {
        log.warn(this.prefix + ": " + message);
    }

    public void trace(String message) {
        log.trace(this.prefix + ": " + message);
    }

    public void resetPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void appendPrefix(String prefix) {
        this.prefix = this.prefix + prefix;
    }

    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    public String getPrefix() {
        return prefix;
    }
}
