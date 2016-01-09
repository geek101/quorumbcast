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

package com.quorum.nio;

import com.quorum.nio.msghelper.ReadMsgCallback;
import com.quorum.util.Callback;
import com.quorum.util.ChannelException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * Helps with parse pipeline for incoming message.
 * Created by powell on 11/10/15.
 */
public class ReadMsgPipeline implements IPipeline<MsgChannel> {
    private List<ReadMsgCallback> readMsgCbList = null;
    private ListIterator iter = null;
    private final Callback owner;
    private Object result = null;

    /**
     * Constructor that takes the owner as argument. Helps with
     * propogating the result.
     * @param owner Caller interface object
     */
    public ReadMsgPipeline(Callback owner,
                           Object ctx) {
        this.owner = owner;
        this.result = ctx;
    }

    public ReadMsgPipeline add(Object o) {
        if (readMsgCbList == null) {
            readMsgCbList = new ArrayList<ReadMsgCallback>();
            readMsgCbList.add((ReadMsgCallback) o);
        } else {
            readMsgCbList.add((ReadMsgCallback) o);
        }
        iter = readMsgCbList.listIterator();
        return this;
    }

    /**
     * Run the next callback in the pipeline
     * @param o com.quorum.Message channel.
     * @throws Exception
     */
    public void runNext(MsgChannel o)
            throws ChannelException, IOException {
        if (iter.hasNext()) {
            ReadMsgCallback cb = (ReadMsgCallback)iter.next();
            cb.readMsg(result, o);
            if (cb.shouldRetry()) {
                iter.previous();   // have to retry this again.
                return;
            } else {
                // store the result to push downward
                this.result = cb.getResult();
            }

            // Check if we are done with the pipeline
            if (!iter.hasNext()) {
                // Notify the owner of this pipeline
                exit();
                return;
            }
        } else {
            throw new ChannelException("No callback found!");
        }
    }

    private void exit() throws ChannelException, IOException {
        owner.call(result);
    }
}
