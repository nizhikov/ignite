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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.nio.file.Paths;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;

/**
 *
 */
public class SnapshotDirectories {
    /** Snapshot name. */
    private final String name;

    /** Optional snapshot path. */
    private final @Nullable String path;

    /** Root snapshot directory. */
    private final File root;

    /** db directory inside root. */
    private final File db;

    /** Directory for temp files. */
    private final File snpTmp;

    /** Folder name. */
    private final String folderName;

    /**
     * Creates snapshot directories based on root directory.
     * @param root Root directory.
     */
    public SnapshotDirectories(File root) {
        this.root = root;
        db = new File(root, DB_DEFAULT_FOLDER);
        name = root.getName();
        path = null;
        snpTmp = null;
        folderName = null;
    }

    /**
     * @param dirs Ignite node directories.
     * @param name Snapshot name.
     * @param path Snapshot path.
     */
    public SnapshotDirectories(NodeFileTree dirs, String name, @Nullable String path) {
        assert dirs != null;
        assert U.alphanumericUnderscore(name) : name;

        root = path == null
            ? new File(dirs.snapshotsRoot(), name)
            : new File(path, name);
        db = new File(root, DB_DEFAULT_FOLDER);
        this.name = name;
        this.path = path;
        this.snpTmp = new File(dirs.snapshotTempRoot(), name);
        this.folderName = dirs.folderName();
    }

    /**
     * @return Snapshot name.
     * @see SnapshotMetadata#snapshotName()
     */
    public String name() {
        return name;
    }

    /**
     * TODO: remove me.
     * @return Snapshot path.
     */
    public String path() {
        return path;
    }

    /**
     * @return Snapshot root directory.
     */
    public File root() {
        return root;
    }

    /**
     * @return Path to the {@code db} directory.
     */
    public File db() {
        return db;
    }

    /**
     * @return Path to the {@code {snapshots}/{snp_name}/db/{folder_name}}.
     */
    public File nodeRoot() {
        return new File(db, folderName);
    }

    /**
     * @return Temp directory for temp files.
     * @see NodeFileTree#snapshotTempRoot()
     */
    public File snapshotTemp() {
        return snpTmp;
    }

    /** @return {snp_tmp}/db/{folder_name} */
    public File snapshotTempWithConsistentId() {
        return Paths.get(snpTmp.getAbsolutePath(), DB_DEFAULT_FOLDER, folderName).toFile();
    }
}
