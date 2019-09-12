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

package org.apache.ignite.codegen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ObjIntConsumer;
import org.apache.ignite.internal.processors.metric.list.walker.Order;
import org.apache.ignite.spi.metric.jmx.MonitoringListMBean;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.view.CacheGroupView;
import org.apache.ignite.spi.metric.list.view.CacheView;
import org.apache.ignite.spi.metric.list.view.ComputeTaskView;
import org.apache.ignite.spi.metric.list.view.ServiceView;
import org.apache.ignite.spi.metric.sql.MonitoringListLocalSystemView;

import static org.apache.ignite.codegen.MessageCodeGenerator.DFLT_SRC_DIR;

/**
 * Application for code generation of {@link MonitoringRowAttributeWalker}.
 * Usage: simply run main method from Ignite source folder(using IDE or other way).
 * Generated code used in {@link MonitoringList}.
 *
 * @see MonitoringListMBean
 * @see MonitoringListLocalSystemView
 */
public class MonitoringRowAttributeWalkerGenerator {
    /** Methods that should be excluded from specific {@link MonitoringRowAttributeWalker}. */
    private static final Set<String> SYS_METHODS = new HashSet<>(Arrays.asList("equals", "hashCode", "toString",
        "getClass"));

    /** Package for {@link MonitoringRowAttributeWalker} implementations. */
    public static final String WALKER_PACKAGE = "org.apache.ignite.internal.processors.metric.list.walker";

    /** */
    public static final String TAB = "    ";

    /**
     * @throws Exception If generation failed.
     */
    public static void main(String[] args) throws Exception {
        MonitoringRowAttributeWalkerGenerator gen = new MonitoringRowAttributeWalkerGenerator();

        gen.generateAndWrite(CacheGroupView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(CacheView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ServiceView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ComputeTaskView.class, DFLT_SRC_DIR);
    }

    /**
     * Generates {@link MonitoringRowAttributeWalker} implementation and write it to the file.
     *
     * @param clazz Class to geneare {@link MonitoringRowAttributeWalker} for.
     * @param srcRoot Source root folder.
     * @param <T> type of the {@link MonitoringRow}.
     * @throws IOException If generation failed.
     */
    private <T extends MonitoringRow> void generateAndWrite(Class<T> clazz, String srcRoot) throws IOException {
        File walkerClass = new File(srcRoot + '/' + WALKER_PACKAGE.replaceAll("\\.", "/") + '/' +
            clazz.getSimpleName() + "Walker.java");

        Collection<String> code = generate(clazz);

        walkerClass.createNewFile();

        try (FileWriter writer = new FileWriter(walkerClass)) {
            for (String line : code) {
                writer.write(line);
                writer.write('\n');
            }
        }
    }

    /**
     * Generates {@link MonitoringRowAttributeWalker} implementation.
     *
     * @param clazz Class to geneare {@link MonitoringRowAttributeWalker} for.
     * @param <T> type of the {@link MonitoringRow}.
     * @return Java source code of the {@link MonitoringRowAttributeWalker} implementation.
     */
    private <T extends MonitoringRow> Collection<String> generate(Class<T> clazz) {
        final List<String> code = new ArrayList<>();
        final Set<String> imports = new TreeSet<>();

        imports.add("import " + MonitoringRowAttributeWalker.class.getName() + ';');
        imports.add("import " + clazz.getName() + ';');

        String simpleName = clazz.getSimpleName();

        code.add("package " + WALKER_PACKAGE + ";");
        code.add("");
        code.add("");
        code.add("/**");
        code.add(" * Generated by {@code " + MonitoringRowAttributeWalkerGenerator.class.getName() + "}.");
        code.add(" * {@link " + simpleName + "} attributes walker.");
        code.add(" * ");
        code.add(" * @see " + simpleName);
        code.add(" */");
        code.add("public class " + simpleName + "Walker implements MonitoringRowAttributeWalker<" + simpleName + "> {");
        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitAll(AttributeVisitor v) {");

        forEachMethod(clazz, (m, i) -> {
            String name = m.getName();

            Class<?> retClazz = m.getReturnType();

            String line = TAB + TAB;

            if (!retClazz.isPrimitive() && !retClazz.getName().startsWith("java.lang"))
                imports.add("import " + retClazz.getName() + ';');

            line += "v.accept(" + i + ", \"" + name + "\", " + retClazz.getSimpleName() + ".class);";

            code.add(line);
        });

        code.add(TAB + "}");
        code.add("");
        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitAll(" + simpleName + " row, AttributeWithValueVisitor v) {");

        forEachMethod(clazz, (m, i) -> {
            String name = m.getName();

            Class<?> retClazz = m.getReturnType();

            String line = TAB + TAB;

            if (!retClazz.isPrimitive()) {
                line += "v.accept(" + i + ", \"" + name + "\", " + retClazz.getSimpleName() + ".class, row." + m.getName() + "());";
            }
            else if (retClazz == boolean.class)
                line += "v.acceptBoolean(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == char.class)
                line += "v.acceptChar(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == byte.class)
                line += "v.acceptByte(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == short.class)
                line += "v.acceptShort(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == int.class)
                line += "v.acceptInt(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == long.class)
                line += "v.acceptLong(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == float.class)
                line += "v.acceptFloat(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == double.class)
                line += "v.acceptDouble(" + i + ", \"" + name + "\", row." + m.getName() + "());";

            code.add(line);
        });

        code.add(TAB + "}");
        code.add("");

        final int[] cnt = {0};
        forEachMethod(clazz, (m, i) -> cnt[0]++);

        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public int count() {");
        code.add(TAB + TAB + "return " + cnt[0] + ';');
        code.add(TAB + "}");
        code.add("}");

        code.addAll(2, imports);

        addLicenseHeader(code);

        return code;
    }

    /**
     * Adds Apache License Header to the source code.
     *
     * @param code Source code.
     */
    private void addLicenseHeader(List<String> code) {
        List<String> lic = new ArrayList<>();

        lic.add("/*");
        lic.add(" * Licensed to the Apache Software Foundation (ASF) under one or more");
        lic.add(" * contributor license agreements.  See the NOTICE file distributed with");
        lic.add(" * this work for additional information regarding copyright ownership.");
        lic.add(" * The ASF licenses this file to You under the Apache License, Version 2.0");
        lic.add(" * (the \"License\"); you may not use this file except in compliance with");
        lic.add(" * the License.  You may obtain a copy of the License at");
        lic.add(" *");
        lic.add(" *      http://www.apache.org/licenses/LICENSE-2.0");
        lic.add(" *");
        lic.add(" * Unless required by applicable law or agreed to in writing, software");
        lic.add(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
        lic.add(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
        lic.add(" * See the License for the specific language governing permissions and");
        lic.add(" * limitations under the License.");
        lic.add(" */");
        lic.add("");

        code.addAll(0, lic);
    }

    /**
     * Iterates each method of the {@code clazz} and call consume {@code c} for it.
     *
     * @param c Method consumer.
     */
    private void forEachMethod(Class<?> clazz, ObjIntConsumer<Method> c) {
        Method[] methods = clazz.getMethods();

        List<Method> notOrdered = new ArrayList<>();
        List<Method> ordered = new ArrayList<>();

        for (int i = 0; i < methods.length; i++) {
            Method m = methods[i];

            if (Modifier.isStatic(m.getModifiers()))
                continue;

            if (SYS_METHODS.contains(m.getName()))
                continue;

            Class<?> retClazz = m.getReturnType();

            if (retClazz == void.class)
                continue;

            if (m.getAnnotation(Order.class) != null)
                ordered.add(m);
            else
                notOrdered.add(m);
        }

        Collections.sort(ordered, Comparator.comparingInt(m -> m.getAnnotation(Order.class).value()));
        Collections.sort(notOrdered, Comparator.comparing(Method::getName));

        for (int i = 0; i < ordered.size(); i++)
            c.accept(ordered.get(i), i);

        for (int i = 0; i < notOrdered.size(); i++)
            c.accept(notOrdered.get(i), i + ordered.size());
    }
}
