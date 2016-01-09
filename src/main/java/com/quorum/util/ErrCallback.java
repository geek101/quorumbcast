package com.quorum.util;

import java.io.IOException;

/**
 * Created by powell on 12/24/15.
 */
public interface ErrCallback {
    void caughtException(Exception exp) throws Exception;
}
