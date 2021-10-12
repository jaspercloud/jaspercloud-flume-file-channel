/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

class EventQueueBackingStoreFactory {
    private static final Logger LOG = LoggerFactory.getLogger(EventQueueBackingStoreFactory.class);

    private EventQueueBackingStoreFactory() {
    }

    static EventQueueBackingStore get(
            File checkpointFile, int capacity, String name) throws Exception {
        return get(checkpointFile, capacity, name, true);
    }

    static EventQueueBackingStore get(
            File checkpointFile, int capacity, String name, boolean upgrade
    ) throws Exception {
        return get(checkpointFile, null, capacity, name, upgrade, false, false);
    }

    static EventQueueBackingStore get(
            File checkpointFile, File backupCheckpointDir, int capacity, String name,
            boolean upgrade, boolean shouldBackup, boolean compressBackup
    ) throws Exception {
        File metaDataFile = Serialization.getMetaDataFile(checkpointFile);
        RandomAccessFile checkpointFileHandle = null;
        try {
            boolean checkpointExists = checkpointFile.exists();
            boolean metaDataExists = metaDataFile.exists();
            if (metaDataExists) {
                // if we have a metadata file but no checkpoint file, we have a problem
                // delete everything in the checkpoint directory and force
                // a full replay.
                if (!checkpointExists || checkpointFile.length() == 0) {
                    LOG.warn("MetaData file for checkpoint "
                            + " exists but checkpoint does not. Checkpoint = " + checkpointFile
                            + ", metaDataFile = " + metaDataFile);
                    throw new BadCheckpointException(
                            "The last checkpoint was not completed correctly, " +
                                    "since Checkpoint file does not exist while metadata " +
                                    "file does.");
                }
            }
            // brand new, use v3
            if (!checkpointExists) {
                if (!checkpointFile.createNewFile()) {
                    throw new IOException("Cannot create " + checkpointFile);
                }
                return new EventQueueBackingStoreFileV3(checkpointFile,
                        capacity, name, backupCheckpointDir, shouldBackup, compressBackup);
            }
            // v3 due to meta file, version will be checked by backing store
            if (metaDataExists) {
                return new EventQueueBackingStoreFileV3(checkpointFile, capacity,
                        name, backupCheckpointDir, shouldBackup, compressBackup);
            }
            checkpointFileHandle = new RandomAccessFile(checkpointFile, "r");
            int version = (int) checkpointFileHandle.readLong();
            if (Serialization.VERSION_2 == version) {
            } else {
                return new EventQueueBackingStoreFileV3(checkpointFile, capacity, name,
                        backupCheckpointDir, shouldBackup, compressBackup);
            }
            LOG.error("Found version " + Integer.toHexString(version) + " in " +
                    checkpointFile);
            throw new BadCheckpointException("Checkpoint file exists with " +
                    Serialization.VERSION_3 + " but no metadata file found.");
        } finally {
            if (checkpointFileHandle != null) {
                try {
                    checkpointFileHandle.close();
                } catch (IOException e) {
                    LOG.warn("Unable to close " + checkpointFile, e);
                }
            }
        }
    }

}
