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

package com.datastax.oss.cdc.backfill;

import com.datastax.oss.cdc.backfill.exporter.TableExporter;
import com.datastax.oss.cdc.backfill.importer.KafkaImporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the export to disk and the import to Kafka activities.
 */
public class CassandraToKafkaMigrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraToKafkaMigrator.class);
    private final TableExporter exporter;
    private final KafkaImporter importer;

    public CassandraToKafkaMigrator(TableExporter exporter, KafkaImporter importer) {
        this.exporter = exporter;
        this.importer = importer;
    }

    public ExitStatus migrate() {
        ExitStatus status = this.exporter.exportTable();
        if (status == ExitStatus.STATUS_OK) {
            LOGGER.info("Sending table records from disk to Kafka.");
            status = this.importer.importTable();
        } else {
            LOGGER.error("Failed to export tables. Sending to Kafka will be skipped.");
        }
        return status;
    }
}
