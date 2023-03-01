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
import com.datastax.oss.cdc.backfill.importer.PulsarImporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CassandraToPulsarMigratorTest {

    @Mock
    private TableExporter exporter;

    @Mock
    private PulsarImporter importer;

    private CassandraToPulsarMigrator migrator;

    @BeforeEach
    public void init() {
        MockitoAnnotations.openMocks(this);

        migrator = new CassandraToPulsarMigrator(exporter, importer);
    }

    @Test
    public void testMigrateHappyPath() {
        // given
        Mockito.when(importer.importTable()).thenReturn(ExitStatus.STATUS_OK);
        Mockito.when(exporter.exportTable()).thenReturn(ExitStatus.STATUS_OK);

        // when
        ExitStatus status = migrator.migrate();

        // then
        Mockito.verify(exporter, Mockito.times(1)).exportTable();
        Mockito.verify(importer, Mockito.times(1)).importTable();

        assertEquals(status, ExitStatus.STATUS_OK);

        Mockito.verifyNoMoreInteractions(importer, exporter);
    }
    @Test
    public void testMigrateExportFailed() {
        // given
        Mockito.when(importer.importTable()).thenReturn(ExitStatus.STATUS_OK);
        Mockito.when(exporter.exportTable()).thenReturn(ExitStatus.STATUS_ABORTED_TOO_MANY_ERRORS);

        // when
        ExitStatus status = migrator.migrate();

        // then
        Mockito.verify(exporter, Mockito.times(1)).exportTable();
        Mockito.verify(importer, Mockito.never()).importTable();

        assertEquals(status, ExitStatus.STATUS_ABORTED_TOO_MANY_ERRORS);

        Mockito.verifyNoMoreInteractions(importer, exporter);
    }
}
