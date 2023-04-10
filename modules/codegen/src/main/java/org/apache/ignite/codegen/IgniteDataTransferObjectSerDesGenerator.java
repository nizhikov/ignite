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
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.CdcDeleteLostSegmentLinksCommand;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.management.api.BaseCommand;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;
import static org.apache.ignite.codegen.MessageCodeGenerator.DFLT_SRC_DIR;
import static org.apache.ignite.codegen.MessageCodeGenerator.TAB;

/**
 *
 */
public class IgniteDataTransferObjectSerDesGenerator {
    /** */
    public static final String METHOD_JAVADOC = TAB + "/** {@inheritDoc} */";

    /** */
    public static final String IMPORT_TOKEN = "import ";

    /** */
    public static final String IMPORT_STATIC_TOKEN = "import static ";

    /** */
    private int methodsStart = Integer.MAX_VALUE;

    /** */
    private final Set<String> imports = new TreeSet<>();

    /** */
    private final Set<String> staticImports = new TreeSet<>();

    /** */
    private static final Map<Class<?>, IgniteBiTuple<Function<String, String>, Function<String, String>>> TYPE_GENS
        = new HashMap<>();

    static {
        //TODO: handle primitive collections.

        TYPE_GENS.put(String.class, F.t(
            fld -> "U.writeString(out, " + fld + ");",
            fld -> fld + " = U.readString(in);"
        ));

        TYPE_GENS.put(boolean.class, F.t(
            fld -> "out.writeBoolean(" + fld + ");",
            fld -> fld + " = in.readBoolean();"
        ));

        TYPE_GENS.put(UUID.class, F.t(
            fld -> "U.writeUuid(out, " + fld + ");",
            fld -> fld + " = U.readUuid(in);"
        ));

        TYPE_GENS.put(Collection.class, F.t(
            fld -> "U.writeCollection(out, " + fld + ");",
            fld -> fld + " = U.readCollection(in);"
        ));
    }

    /**
     * @param args Command line arguments.
     * @throws Exception If generation failed.
     */
    public static void main(String[] args) throws Exception {
        IgniteDataTransferObjectSerDesGenerator gen = new IgniteDataTransferObjectSerDesGenerator();

        gen.generateAndWrite(SystemViewCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(CdcDeleteLostSegmentLinksCommand.class, DFLT_SRC_DIR);
    }

    /** */
    private void generateAndWrite(Class<? extends Command> cls, String srcDir) throws IOException {
        clear();

        File file = new File(srcDir, cls.getName().replace('.', File.separatorChar) + ".java");

        if (!file.exists() || !file.isFile())
            throw new IllegalArgumentException("Source file not found: " + file.getPath());

        List<String> src = removeExisting(Files.readAllLines(file.toPath(), StandardCharsets.UTF_8));

        List<List<String>> readWriteMethods = generateMethods(cls);

        List<String> code = new ArrayList<>();

        int i = 0;

        // Outputs all before imports.
        for (; i < src.size(); i++) {
            code.add(src.get(i));

            if (src.get(i).startsWith("package"))
                break;
        }

        code.add("");

        i++;

        for (String imp : imports)
            code.add("import " + imp);

        for (String imp : staticImports)
            code.add("import static " + imp);

        i++;

        for (; i < methodsStart; i++)
            code.add(src.get(i));

        code.add("");
        code.addAll(readWriteMethods.get(0));
        code.add("");
        code.addAll(readWriteMethods.get(1));

        for (; i < src.size(); i++)
            code.add(src.get(i));

        try (FileWriter writer = new FileWriter(file)) {
            for (String line : code) {
                writer.write(line);
                writer.write('\n');
            }
        }
    }

    /** */
    private List<List<String>> generateMethods(Class<? extends Command> cls) {
        List<String> write = new ArrayList<>();
        List<String> read = new ArrayList<>();

        write.add(METHOD_JAVADOC);
        write.add(TAB + "@Override protected void writeExternalData(ObjectOutput out) throws IOException {");

        read.add(METHOD_JAVADOC);
        read.add(TAB + "@Override protected void readExternalData(byte protoVer, ObjectInput in) " +
            "throws IOException, ClassNotFoundException {");

        imports.add(U.class.getName() + ';');

        if (cls.getSuperclass() != IgniteDataTransferObject.class || cls.getSuperclass() != BaseCommand.class) {
            write.add(TAB + TAB + "super.writeExternalData(out);");
            write.add("");

            read.add(TAB + TAB + "super.readExternalData(protoVer, in);");
            read.add("");
        }

        Field[] flds = cls.getDeclaredFields();

        if (flds.length == 0) {
            write.add(TAB + TAB + "// No-op.");
            read.add(TAB + TAB + "// No-op.");
        }
        else {
            for (Field fld : flds) {
                int mod = fld.getModifiers();

                if (isStatic(mod) || isTransient(mod))
                    continue;

                if (!TYPE_GENS.containsKey(fld.getType()))
                    throw new IllegalArgumentException(fld.getType() + " not supported");

                write.add(TAB + TAB + TYPE_GENS.get(fld.getType()).get1().apply(fld.getName()));
                read.add(TAB + TAB + TYPE_GENS.get(fld.getType()).get2().apply(fld.getName()));
            }
        }

        write.add(TAB + "}");
        read.add(TAB + "}");

        return Arrays.asList(write, read);
    }

    /** */
    private List<String> removeExisting(List<String> src) {
        return removeMethod(
            removeMethod(
                collectAndRemoveImports(src),
                "writeExternalData"
            ),
            "readExternalData"
        );
    }

    /** */
    private List<String> removeMethod(List<String> src, String methodName) {
        int start = -1;
        int finish = -1;
        int bracketCnt = -1;

        for (int i = 0; i < src.size(); i++) {
            String line = src.get(i);

            if (line.contains(methodName) && line.endsWith("{")) {
                assert src.get(i - 1).equals(METHOD_JAVADOC);

                // One line for comment and one for empty line between methods.
                start = i - 2;
                bracketCnt = 1;
            }
            else if (start != -1) {
                bracketCnt += counfOf(line, '{') - counfOf(line, '}');

                if (bracketCnt < 0)
                    throw new IllegalStateException("Wrong brackets count");

                if (bracketCnt == 0) {
                    finish = i;
                    break;
                }
            }
        }

        if (start == -1 || finish == -1)
            throw new IllegalStateException("Method bounds not found");

        methodsStart = start;

        List<String> res = new ArrayList<>(src.subList(0, start));

        res.addAll(src.subList(finish + 1, src.size()));

        return res;
    }

    /** */
    private List<String> collectAndRemoveImports(List<String> src) {
        return src.stream()
            .peek(line -> {
                if (line.startsWith(IMPORT_STATIC_TOKEN))
                    staticImports.add(line.substring(IMPORT_STATIC_TOKEN.length()));
                else if (line.startsWith(IMPORT_TOKEN))
                    imports.add(line.substring(IMPORT_TOKEN.length()));
            })
            .filter(line -> !line.startsWith(IMPORT_TOKEN))
            .collect(Collectors.toList());
    }

    /** */
    private int counfOf(String line, char ch) {
        int cnt = 0;
        int idx = line.indexOf(ch);

        while (idx != -1) {
            cnt++;
            idx = line.indexOf(ch, idx + 1);
        }

        return cnt;
    }

    /** */
    private void clear() {
        methodsStart = Integer.MAX_VALUE;
        imports.clear();
        staticImports.clear();
    }
}
