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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_USE_TYPED_ARRAYS;

/**
 * Binary object representing array.
 */
public class BinaryArray implements BinaryObjectEx, Externalizable {
    /** Default value of {@link IgniteSystemProperties#IGNITE_USE_TYPED_ARRAYS}. */
    public static final boolean DFLT_IGNITE_USE_TYPED_ARRAYS = true;

    /** Value of {@link IgniteSystemProperties#IGNITE_USE_TYPED_ARRAYS}. */
    public static boolean USE_TYPED_ARRAYS =
        IgniteSystemProperties.getBoolean(IGNITE_USE_TYPED_ARRAYS, DFLT_IGNITE_USE_TYPED_ARRAYS);

    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    @GridDirectTransient
    @GridToStringExclude
    private BinaryContext ctx;

    /** Type ID. */
    private int compTypeId;

    /** Type class name. */
    @Nullable private String compClsName;

    /** Values. */
    @GridToStringInclude(sensitive = true)
    private Object[] arr;

    /** */
    private boolean keepBinary;

    /**
     * {@link Externalizable} support.
     */
    public BinaryArray() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param compTypeId Component type id.
     * @param compClsName Component class name.
     * @param arr Array.
     * @param keepBinary Keep binary.
     */
    public BinaryArray(BinaryContext ctx, int compTypeId, @Nullable String compClsName, Object[] arr, boolean keepBinary) {
        this.ctx = ctx;
        this.compTypeId = compTypeId;
        this.compClsName = compClsName;
        this.arr = arr;
        this.keepBinary = keepBinary;
    }

    /** {@inheritDoc} */
    @Override public BinaryType type() throws BinaryObjectException {
        return BinaryUtils.typeProxy(ctx, this);
    }

    /** {@inheritDoc} */
    @Override public @Nullable BinaryType rawType() throws BinaryObjectException {
        return BinaryUtils.type(ctx, this);
    }

    /** {@inheritDoc} */
    @Override public <T> T deserialize() throws BinaryObjectException {
        return (T)deserialize(null);
    }

    /** {@inheritDoc} */
    @Override public <T> T deserialize(ClassLoader ldr) throws BinaryObjectException {
        if (keepBinary)
            return (T)arr;

        ClassLoader resolveLdr = ldr == null ? ctx.configuration().getClassLoader() : ldr;

        if (ldr != null)
            GridBinaryMarshaller.USE_CACHE.set(Boolean.FALSE);

        try {
            Class<?> compType = BinaryUtils.resolveClass(ctx, compTypeId, compClsName, resolveLdr, false);

            Object[] res = Object.class == compType ? arr : (Object[])Array.newInstance(compType, arr.length);

            boolean keepBinary = BinaryObject.class.isAssignableFrom(compType);

            for (int i = 0; i < arr.length; i++) {
                Object obj = CacheObjectUtils.unwrapBinaryIfNeeded(null, arr[i], keepBinary, false, ldr);

                if (!keepBinary && obj != null && BinaryObject.class.isAssignableFrom(obj.getClass()))
                    obj = ((BinaryObject)obj).deserialize(ldr);

                res[i] = obj;
            }

            return (T)res;
        }
        finally {
            GridBinaryMarshaller.USE_CACHE.set(Boolean.TRUE);
        }
    }

    /**
     * @return Underlying array.
     */
    public Object[] array() {
        return arr;
    }

    /** @param keepBinary Keep binary value. */
    public void keepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
    }

    /**
     * @return Component type ID.
     */
    public int componentTypeId() {
        return compTypeId;
    }

    /**
     * @return Component class name.
     */
    public String componentClassName() {
        return compClsName;
    }

    /** {@inheritDoc} */
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return new BinaryArray(ctx, compTypeId, compClsName, arr.clone(), keepBinary);
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return GridBinaryMarshaller.OBJ_ARR;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(compTypeId);
        out.writeObject(compClsName);
        out.writeObject(arr);
        out.writeBoolean(keepBinary);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = GridBinaryMarshaller.threadLocalContext();

        compTypeId = in.readInt();
        compClsName = (String)in.readObject();
        arr = (Object[])in.readObject();
        keepBinary = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder toBuilder() throws BinaryObjectException {
        throw new UnsupportedOperationException("Builder cannot be created for array wrapper.");
    }

    /** {@inheritDoc} */
    @Override public int enumOrdinal() throws BinaryObjectException {
        throw new BinaryObjectException("Object is not enum.");
    }

    /** {@inheritDoc} */
    @Override public String enumName() throws BinaryObjectException {
        throw new BinaryObjectException("Object is not enum.");
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return -1; //TODO: fixme
    }

    /** {@inheritDoc} */
    @Override public boolean isFlagSet(short flag) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <F> F field(String fieldName) throws BinaryObjectException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(String fieldName) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = 31 * Objects.hash(compTypeId);

        // {@link Arrays#deepHashCode(Object[])} used because array elements can be array of primitives
        // or supported types like String, UUID, etc. "Standart" arrays like int[], String[] not modified
        // during binarization - {@link CacheObjectBinaryProcessorImpl#marshalToBinary(Object, boolean)}.
        // See {@link BinaryUtils#BINARY_CLS} for details.
        result = 31 * result + Arrays.deepHashCode(arr);

        return result;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        BinaryArray arr = (BinaryArray)o;

        return compTypeId == arr.compTypeId
            && Arrays.equals(this.arr, arr.arr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryArray.class, this);
    }
}
