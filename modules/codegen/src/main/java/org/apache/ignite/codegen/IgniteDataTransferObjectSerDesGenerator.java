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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.MetricCommandArg;
import org.apache.ignite.internal.management.MetricConfigureHistogramCommandArg;
import org.apache.ignite.internal.management.MetricConfigureHitrateCommandArg;
import org.apache.ignite.internal.management.SystemViewCommandArg;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
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
    private static final Map<Class<?>, GridTuple3<Function<String, String>, Function<String, String>, Boolean>> TYPE_GENS
        = new HashMap<>();

    static {
        TYPE_GENS.put(byte.class, F.t(
            fld -> "out.writeByte(" + fld + ");",
            fld -> fld + " = in.readByte();",
            false
        ));

        TYPE_GENS.put(byte[].class, F.t(
            fld -> "U.writeByteArray(out, " + fld + ");",
            fld -> fld + " = U.readByteArray(in);",
            true
        ));

        TYPE_GENS.put(short.class, F.t(
            fld -> "out.writeShort(" + fld + ");",
            fld -> fld + " = in.readShort();",
            false
        ));

        TYPE_GENS.put(short[].class, F.t(
            fld -> "U.writeShortArray(out, " + fld + ");",
            fld -> fld + " = U.readShortArray(in);",
            true
        ));

        TYPE_GENS.put(int.class, F.t(
            fld -> "out.writeInt(" + fld + ");",
            fld -> fld + " = in.readInt();",
            false
        ));

        TYPE_GENS.put(int[].class, F.t(
            fld -> "U.writeIntArray(out, " + fld + ");",
            fld -> fld + " = U.readIntArray(in);",
            true
        ));

        TYPE_GENS.put(long.class, F.t(
            fld -> "out.writeLong(" + fld + ");",
            fld -> fld + " = in.readLong();",
            false
        ));

        TYPE_GENS.put(long[].class, F.t(
            fld -> "U.writeLongArray(out, " + fld + ");",
            fld -> fld + " = U.readLongArray(in);",
            true
        ));

        TYPE_GENS.put(float.class, F.t(
            fld -> "out.writeFloat(" + fld + ");",
            fld -> fld + " = in.readFloat();",
            false
        ));

        TYPE_GENS.put(float[].class, F.t(
            fld -> "U.writeFloatArray(out, " + fld + ");",
            fld -> fld + " = U.readFloatArray(in);",
            true
        ));

        TYPE_GENS.put(double.class, F.t(
            fld -> "out.writeDouble(" + fld + ");",
            fld -> fld + " = in.readDouble();",
            false
        ));

        TYPE_GENS.put(double[].class, F.t(
            fld -> "U.writeDoubleArray(out, " + fld + ");",
            fld -> fld + " = U.readDoubleArray(in);",
            true
        ));

        TYPE_GENS.put(boolean.class, F.t(
            fld -> "out.writeBoolean(" + fld + ");",
            fld -> fld + " = in.readBoolean();",
            false
        ));

        TYPE_GENS.put(boolean[].class, F.t(
            fld -> "U.writeBooleanArray(out, " + fld + ");",
            fld -> fld + " = U.readBooleanArray(in);",
            true
        ));

        TYPE_GENS.put(String.class, F.t(
            fld -> "U.writeString(out, " + fld + ");",
            fld -> fld + " = U.readString(in);",
            true
        ));

        TYPE_GENS.put(UUID.class, F.t(
            fld -> "U.writeUuid(out, " + fld + ");",
            fld -> fld + " = U.readUuid(in);",
            true
        ));

        TYPE_GENS.put(IgniteUuid.class, F.t(
            fld -> "U.writeIgniteUuid(out, " + fld + ");",
            fld -> fld + " = U.readIgniteUuid(in);",
            true
        ));

        TYPE_GENS.put(Collection.class, F.t(
            fld -> "U.writeCollection(out, " + fld + ");",
            fld -> fld + " = U.readCollection(in);",
            true
        ));

        TYPE_GENS.put(List.class, F.t(
            fld -> "U.writeCollection(out, " + fld + ");",
            fld -> fld + " = U.readList(in);",
            true
        ));

        TYPE_GENS.put(Set.class, F.t(
            fld -> "U.writeCollection(out, " + fld + ");",
            fld -> fld + " = U.readSet(in);",
            true
        ));

        TYPE_GENS.put(GridCacheVersion.class, F.t(
            fld -> "out.writeObject(" + fld + ");",
            fld -> fld + " = (GridCacheVersion)in.readObject();",
            false
        ));
    }

    /**
     * @param args Command line arguments.
     * @throws Exception If generation failed.
     */
    public static void main(String[] args) throws Exception {
        IgniteDataTransferObjectSerDesGenerator gen = new IgniteDataTransferObjectSerDesGenerator();

        gen.generateAndWrite(MetricCommandArg.class, DFLT_SRC_DIR);
        gen.generateAndWrite(MetricConfigureHistogramCommandArg.class, DFLT_SRC_DIR);
        gen.generateAndWrite(MetricConfigureHitrateCommandArg.class, DFLT_SRC_DIR);
        gen.generateAndWrite(SystemViewCommandArg.class, DFLT_SRC_DIR);
/*
        gen.generateAndWrite(CdcDeleteLostSegmentLinksCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(CdcResendCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ChangeTagCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ConsistencyRepairCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(DeactivateCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(DefragmentationScheduleCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(SetStateCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ShutdownPolicyCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(TxCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(TxInfoCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(WarmUpCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(WalDeleteCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(WalPrintCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(TracingConfigurationGetAllCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(TracingConfigurationGetCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(TracingConfigurationResetAllCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(TracingConfigurationResetCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(TracingConfigurationSetCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(SnapshotCancelCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(SnapshotCheckCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(SnapshotCreateCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(SnapshotRestoreCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PropertyGetCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PropertySetCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PersistenceBackupCachesCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PersistenceCleanCachesCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(MetaDetailsCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(MetaRemoveCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(MetaUpdateCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(KillClientCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(KillComputeCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(KillContinuousCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(KillScanCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(KillServiceCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(KillSnapshotCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(KillSqlCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(KillTransactionCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(EncryptionCacheKeyIdsCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(EncryptionChangeCacheKeyCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(EncryptionChangeMasterKeyCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(EncryptionReencryptionRateLimitCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(EncryptionReencryptionStatusCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(EncryptionResumeReencryptionCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(EncryptionSuspendReencryptionCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(BaselineAddCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(BaselineAutoAdjustCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(BaselineCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(BaselineRemoveCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(BaselineSetCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(BaselineVersionCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(EncryptionGetMasterKeyNameCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(EncryptionCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(TracingConfigurationCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(SnapshotStatusCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(SnapshotCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(DefragmentationCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(CacheHelpCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(WalCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PropertyCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PropertyListCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PropertyHelpCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(StateCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(CacheCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(MetaCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(MetaListCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(MetaHelpCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(KillCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PersistenceCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PersistenceBackupCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PersistenceBackupCorruptedCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PersistenceBackupAllCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PersistenceInfoCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PersistenceCleanAllCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PersistenceCleanCorruptedCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PersistenceCleanCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ConsistencyFinalizeCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ConsistencyCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(CdcCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(DefragmentationCancelCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(DiagnosticCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ConsistencyStatusCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PerformanceStatisticsCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PerformanceStatisticsStartCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PerformanceStatisticsRotateCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PerformanceStatisticsStopCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PerformanceStatisticsStatusCommand.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ActivateCommand.class, DFLT_SRC_DIR);
*/
    }

    /** */
    private void generateAndWrite(Class<? extends IgniteDataTransferObject> cls, String srcDir) throws IOException {
        clear();

        File file = new File(srcDir, cls.getName().replace('.', File.separatorChar) + ".java");

        if (!file.exists() || !file.isFile())
            throw new IllegalArgumentException("Source file not found: " + file.getPath());

        List<String> src =
            removeExisting(addSerialVersionUID(Files.readAllLines(file.toPath(), StandardCharsets.UTF_8)));

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

        if (methodsStart == Integer.MAX_VALUE) // If methods not exists add them to the end.
            methodsStart = src.size() - 1;

        for (; i < methodsStart; i++)
            code.add(src.get(i));

        if (!readWriteMethods.get(0).isEmpty()) {
            code.add("");
            code.addAll(readWriteMethods.get(0));
        }

        if (!readWriteMethods.get(1).isEmpty()) {
            code.add("");
            code.addAll(readWriteMethods.get(1));
        }

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
    private List<String> addSerialVersionUID(List<String> src) {
        int classStart = -1;

        for (int i = 0; i < src.size(); i++) {
            String line = src.get(i);

            if (line.contains("private static final long serialVersionUID = "))
                return src;

            if (line.startsWith("public class "))
                classStart = i;
        }

        if (classStart == -1)
            return src;

        List<String> res = new ArrayList<>(src.subList(0, classStart + 1));

        res.add(TAB + "/** */");
        res.add(TAB + "private static final long serialVersionUID = 0;");
        res.add("");
        res.addAll(src.subList(classStart + 1, src.size()));

        return res;
    }

    /** */
    private List<List<String>> generateMethods(Class<? extends IgniteDataTransferObject> cls) {
        Field[] flds = cls.getDeclaredFields();

        if (flds.length == 0)
            return Arrays.asList(Collections.emptyList(), Collections.emptyList());

        List<String> write = new ArrayList<>();
        List<String> read = new ArrayList<>();

        write.add(METHOD_JAVADOC);
        write.add(TAB + "@Override protected void writeExternalData(ObjectOutput out) throws IOException {");

        read.add(METHOD_JAVADOC);
        read.add(TAB + "@Override protected void readExternalData(byte protoVer, ObjectInput in) " +
            "throws IOException, ClassNotFoundException {");

        imports.add(IOException.class.getName() + ';');
        imports.add(ObjectOutput.class.getName() + ';');
        imports.add(ObjectInput.class.getName() + ';');

        if (cls.getSuperclass() != IgniteDataTransferObject.class) {
            write.add(TAB + TAB + "super.writeExternalData(out);");
            write.add("");

            read.add(TAB + TAB + "super.readExternalData(protoVer, in);");
            read.add("");
        }

        for (Field fld : flds) {
            int mod = fld.getModifiers();

            if (isStatic(mod) || isTransient(mod))
                continue;

            String name = fld.getName();

            if (Enum.class.isAssignableFrom(fld.getType())) {
                imports.add(U.class.getName() + ';');

                write.add(TAB + TAB + "U.writeEnum(out, " + (name.equals("out") ? "this.out." : "") + name + ");");
                read.add(TAB + TAB + (name.equals("in") ? "this." : "") + name +
                    " = U.readEnum(in, " + fld.getType().getSimpleName() + ".class);");
            }
            else {
                GridTuple3<Function<String, String>, Function<String, String>, Boolean> gen
                    = TYPE_GENS.get(fld.getType());

                if (gen == null)
                    throw new IllegalArgumentException(fld.getType() + " not supported[cls=" + cls.getName() + ']');

                if (gen.get3())
                    imports.add(U.class.getName() + ';');

                write.add(TAB + TAB + gen.get1().apply((name.equals("out") ? "this." : "") + name));
                read.add(TAB + TAB + gen.get2().apply((name.equals("in") ? "this." : "") + name));
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
            return src;

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
