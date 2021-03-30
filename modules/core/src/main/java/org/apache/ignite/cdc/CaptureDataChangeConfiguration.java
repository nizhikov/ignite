/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cdc;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * This class defines {@link IgniteCDC} runtime configuration.
 * This configuration is passed to {@link IgniteCDC} constructor.
 * It defines all configuration
 */
@IgniteExperimental
public class CaptureDataChangeConfiguration {
    /** Capture Data Change consumer. */
    private CaptureDataChangeConsumer<?, ?> consumer;

    /** Keep binary flag.<br>Default value {@code true}. */
    private boolean keepBinary = true;

    /**
     * {@link IgniteCDC} acquire file lock on startup to ensure exclusive consumption.
     * This property specifies amount of time to wait for lock acquisition.<br>
     * Default is {@code 1000 ms}.
     */
    private long lockTimeout = 1000;

    /**
     * CDC application periodically scans {@link DataStorageConfiguration#getCdcPath()} folder to find new WAL segments.
     * This timeout specify amount of time application sleeps between subsequent checks when no new files available.<br>
     * Default is {@code 1000 ms}.
     */
    private long sleepBeforeCheckNewSegmentsTimeout = 1000;

    /** @return CDC consumer. */
    public CaptureDataChangeConsumer<?, ?> getConsumer() {
        return consumer;
    }

    /** @param consumer CDC consumer. */
    public void setConsumer(CaptureDataChangeConsumer<?, ?> consumer) {
        this.consumer = consumer;
    }

    /** @return keep binary value. */
    public boolean isKeepBinary() {
        return keepBinary;
    }

    /** @param keepBinary keep binary value. */
    public void setKeepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
    }

    /** @return Amount of time to wait for lock acquisition. */
    public long getLockTimeout() {
        return lockTimeout;
    }

    /** @param lockTimeout Amount of time to wait for lock acquisition. */
    public void setLockTimeout(long lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    /** @return Amount of time application sleeps between subsequent checks when no new files available. */
    public long getSleepBeforeCheckNewSegmentsTimeout() {
        return sleepBeforeCheckNewSegmentsTimeout;
    }

    /**
     * @param sleepBeforeCheckNewSegmentsTimeout Amount of time application sleeps between subsequent checks when no new
     *                                           files available.
     */
    public void setSleepBeforeCheckNewSegmentsTimeout(long sleepBeforeCheckNewSegmentsTimeout) {
        this.sleepBeforeCheckNewSegmentsTimeout = sleepBeforeCheckNewSegmentsTimeout;
    }
}
