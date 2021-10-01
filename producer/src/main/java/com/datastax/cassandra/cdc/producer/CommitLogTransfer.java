/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cassandra.cdc.producer;

import java.nio.file.Path;
import java.util.Properties;

/**
 * Interface used to transfer commit logs
 */
public interface CommitLogTransfer extends AutoCloseable {

    /**
     * Initialize resources required by the commit log transfer
     */
    default void init(Properties commitLogTransferConfigs) throws Exception {
    }

    @Override
    default void close() {
    }

    /**
     * Transfer a commit log that has been successfully processed.
     */
    void onSuccessTransfer(Path file);

    /**
     * Transfer a commit log that has not been successfully processed.
     */
    void onErrorTransfer(Path file);

    /**
     * Get all error commitLog files into cdc_raw directory for re-processing.
     */
    void recycleErrorCommitLogFiles(Path cdcDir);
}
