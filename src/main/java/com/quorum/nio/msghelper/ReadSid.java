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

package com.quorum.nio.msghelper;

import com.quorum.util.ChannelException;
import com.quorum.util.InitMessageCtx;

import java.io.IOException;

/**
 * Read 8byte sid.
 */
public class ReadSid extends ReadNumber<Long, InitMessageCtx> {
    private static Long t = Long.MAX_VALUE;
    private InitMessageCtx resultMsgCtx = null;
    public ReadSid() throws ChannelException, IOException {
        super(t);
    }

    @Override
    public void ctxPreRead(Object ctx) {}

    @Override
    public void ctxPostRead(Object ctx) throws ChannelException, IOException {
        InitMessageCtx initMsgCtx = (InitMessageCtx) ctx;
        initMsgCtx.sid =  getBuf();
        resultMsgCtx = initMsgCtx;
    }

    @Override
    public InitMessageCtx getResult() {
        return resultMsgCtx;
    }
}
