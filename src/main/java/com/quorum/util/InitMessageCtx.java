/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>http://www.apache.org/licenses/LICENSE-2.0</p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.quorum.util;

import io.netty.util.AttributeKey;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Created by powell on 11/11/15.
 */
public class InitMessageCtx {
    public final static AttributeKey<InitMessageCtx> KEY = AttributeKey.valueOf
            ("initmessagectx");
    /*
     * Protocol identifier used among peers
    */
    public static final long PROTOCOL_VERSION = -65536L;
    public Long protocolVersion = null;
    public Long sid = null;
    public Integer remainder = null;
    public InetSocketAddress addr = null;

    /**
     * Helper to get the string passed for addr in
     * the header.
     * @param addr
     * @return
     */
    public static String getAddrString(final InetSocketAddress addr) {
        InetAddress ipAddr = addr.getAddress();
        int port = addr.getPort();
        String ipAddrStr = ipAddr.getHostAddress();
        return ipAddrStr + ":" + String.valueOf(port);
    }
}
