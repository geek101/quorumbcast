package com.quorum.util;

import org.slf4j.Logger;

public class NotNull {
    public static void check(final Object t, final String msg,
                             final Logger log) {
        if (t == null) {
            NullPointerException e = new NullPointerException(msg);
            log.error(msg + ", exp: " + e);
            throw e;
        }
    }

    public static void check(final Object t, final String msg,
                             final LogPrefix log) {
        if (t == null) {
            NullPointerException e = new NullPointerException(msg);
            log.error(msg + ", exp: " + e);
            throw e;
        }
    }
}
