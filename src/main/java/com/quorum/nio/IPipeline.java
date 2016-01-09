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

import com.quorum.util.ChannelException;

import java.io.IOException;

/**
 * Interface for all com.quorum.util.Callback pipelines
 * Created by powell on 11/10/15.
 */
public interface IPipeline<T> {
    IPipeline add(Object o);       /// Add a generic callback object
    void runNext(T o) throws ChannelException, IOException;   /// Run the next
    // handler in the
                                                // chain
}

