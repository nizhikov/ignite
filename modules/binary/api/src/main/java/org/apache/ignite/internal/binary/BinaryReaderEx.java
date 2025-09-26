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

package org.apache.ignite.internal.binary;

import java.io.ObjectInput;
import java.lang.reflect.Field;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.jetbrains.annotations.Nullable;

/**
 * Extended reader interface.
 */
public interface BinaryReaderEx extends BinaryReader, BinaryRawReader, BinaryReaderHandlesHolder, ObjectInput {
    /**
     * @return Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable public Object readObjectDetached() throws BinaryObjectException;

    /**
     * @param deserialize {@code True} if object should be deserialized during reading.
     * @return Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable public Object readObjectDetached(boolean deserialize) throws BinaryObjectException;

    /**
     * @return Input stream.
     */
    public BinaryInputStream in();

    /**
     * @param offset Offset in the array.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    public Object unmarshal(int offset);

    /**
     * @return Deserialized object.
     * @throws BinaryObjectException If failed.
     */
    public Object deserialize() throws BinaryObjectException;

    /**
     * @return Descriptor.
     */
    public BinaryClassDescriptor descriptor();

    /**
     * @param fieldName Field name.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    public Object unmarshalField(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldId Field ID.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    public Object unmarshalField(int fieldId) throws BinaryObjectException;

    /**
     * Try finding the field by name.
     *
     * @param name Field name.
     * @return Offset.
     */
    public boolean findFieldByName(String name);

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    byte readByte(int fieldId) throws BinaryObjectException;

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    boolean readBoolean(int fieldId) throws BinaryObjectException;

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    short readShort(int fieldId) throws BinaryObjectException;

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    char readChar(int fieldId) throws BinaryObjectException;

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    int readInt(int fieldId) throws BinaryObjectException;

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    long readLong(int fieldId) throws BinaryObjectException;

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    float readFloat(int fieldId) throws BinaryObjectException;

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    double readDouble(int fieldId) throws BinaryObjectException;

    /**
     * @param fieldId Field ID.
     * @return Deserialized object.
     * @throws BinaryObjectException If failed.
     */
    @Nullable Object readField(int fieldId) throws BinaryObjectException;

    /**
     * Reads fixed type from the given reader with flags validation.
     *
     * @param mode Binary write mode.
     * @return Read value.
     * @throws BinaryObjectException If failed to read value from the stream.
     */
    Object readFixedType(int id, BinaryWriteMode mode, Field field) throws BinaryObjectException;

    /**
     * Get or create object schema.
     *
     * @return Schema.
     */
    BinarySchema getOrCreateSchema();
}
