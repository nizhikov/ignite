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

import java.util.Arrays;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_USE_TYPED_ARRAYS;
import static org.apache.ignite.internal.binary.BinaryArray.DFLT_IGNITE_USE_TYPED_ARRAYS;

/** Base test to check both mode for {@link IgniteSystemProperties#IGNITE_USE_TYPED_ARRAYS}. */
@RunWith(Parameterized.class)
public class AbstractTypedArrayTest extends GridCommonAbstractTest {
    /** Generates values for the {@link #useTypedArrays} parameter. */
    @Parameterized.Parameters(name = "useTypedArrays = {0}")
    public static Iterable<Object[]> useTypedArrays() {
        return Arrays.asList(new Object[][] {{true}, {false}});
    }

    /** @see IgniteSystemProperties#IGNITE_USE_TYPED_ARRAYS */
    @Parameterized.Parameter
    public boolean useTypedArrays;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IGNITE_USE_TYPED_ARRAYS, Boolean.toString(useTypedArrays));
        BinaryArray.USE_TYPED_ARRAYS = useTypedArrays;

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(IGNITE_USE_TYPED_ARRAYS);
        BinaryArray.USE_TYPED_ARRAYS = DFLT_IGNITE_USE_TYPED_ARRAYS;
    }
}
