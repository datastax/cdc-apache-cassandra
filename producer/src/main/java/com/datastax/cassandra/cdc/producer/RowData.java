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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Row-level data about the source event. Contains a map where the key is the table column
 * name and the value is the {@link CellData}.
 */
public class RowData {
    private final Map<String, CellData> cellMap = new LinkedHashMap<>();

    public void addCell(CellData cellData) {
        this.cellMap.put(cellData.name, cellData);
    }

    public void removeCell(String columnName) {
        if (hasCell(columnName)) {
            cellMap.remove(columnName);
        }
    }

    public boolean hasCell(String columnName) {
        return cellMap.containsKey(columnName);
    }

    public RowData copy() {
        RowData copy = new RowData();
        for (CellData cellData : cellMap.values()) {
            copy.addCell(cellData);
        }
        return copy;
    }

    public List<CellData> primaryKeyCells() {
        return this.cellMap.values().stream().filter(CellData::isPrimary).collect(Collectors.toList());
    }

    public Object[] primaryKeyValues() {
        return this.cellMap.values().stream()
                .filter(CellData::isPrimary)
                .map(cell->cell.value)
                .toArray(Object[]::new);
    }

    public String[] primaryKeyNames() {
        return this.cellMap.values().stream()
                .filter(CellData::isPrimary)
                .map(cell->cell.name)
                .toArray(String[]::new);
    }

    public String[] nonPrimaryKeyNames() {
        return this.cellMap.values().stream()
                .filter(cd -> !cd.isPrimary())
                .map(cell->cell.name)
                .toArray(String[]::new);
    }

    @Override
    public String toString() {
        return this.cellMap.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowData rowData = (RowData) o;
        return Objects.equals(cellMap, rowData.cellMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cellMap);
    }
}
