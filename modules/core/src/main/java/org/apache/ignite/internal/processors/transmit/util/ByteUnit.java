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

package org.apache.ignite.internal.processors.transmit.util;

/**
 * A {@code ByteUnit} represents the number of bytes at a given unit of
 * granularity and provides utility methods to convert across units,
 * and to perform setting amount of bytes for some operations over these
 * units.  A {@code ByteUnit} does not maintain byte information itself,
 * but only helps organize and use the given amount of bytes representations
 * that may be maintained separately across various contexts.
 */
public enum ByteUnit {
    /** Byte type. */
    BYTE(1),

    /** Kilobyte type. */
    KB(1024),

    /** Megabyte type. */
    MB(1024 * 1024),

    /** Gigabyte type. */
    GB(1024 * 1024 * 1024);

    /** The amount of bytes. */
    private final long amount;

    /**
     * @param amount The amount of bytes per configured type.
     */
    ByteUnit(int amount) {
        this.amount = amount;
    }

    /**
     * Translate the amount of bytes using given unit type. For instance,
     * {@code long bytes = BYTE.convertFrom(1, MB)} will be equal to <tt>1048576</tt> bytes.
     *
     * @param amount The amount of this type.
     * @param unit The unit type.
     * @return The amount of bytes corresponding to type.
     */
    public long convertFrom(long amount, ByteUnit unit) {
        return unit.convertTo(amount, this);
    }

    /**
     * Convert given amount of type as this unit type.
     *
     * @param amount The amount of this type.
     * @param unit The unit type.
     * @return The amount of bytes corresponding to type.
     */
    public long convertTo(long amount, ByteUnit unit) {
        if (this.amount > unit.amount) {
            long ratio = this.amount / unit.amount;

            if (Long.MAX_VALUE / ratio < amount)
                throw new IllegalArgumentException("The calculated amount of bytes exceeds the Long.MAX_VALUE for the unit " +
                    "[amount=" + amount + ", unit=" + name() + ']');

            return amount * ratio;
        }
        else
            return amount / (unit.amount / this.amount);
    }

    /**
     * @param amount The amount of this type.
     * @return The amount of bytes corresponding to type.
     */
    public long toBytes(long amount) {
        assert amount >= 0;

        return amount * this.amount;
    }

    /**
     * @param amount The amount of this type.
     * @return The amount of bytes corresponding to type.
     */
    public long toKB(long amount) {
        return convertTo(amount, KB);
    }

    /**
     * @param amount The amount of this type.
     * @return The amount of bytes corresponding to type.
     */
    public long toMB(long amount) {
        return convertTo(amount, MB);
    }

    /**
     * @param amount The amount of this type.
     * @return The amount of bytes corresponding to type.
     */
    public long toGB(long amount) {
        return convertTo(amount, GB);
    }
}
