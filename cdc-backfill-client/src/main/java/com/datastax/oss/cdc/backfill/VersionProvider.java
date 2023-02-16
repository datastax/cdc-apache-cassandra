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

import picocli.CommandLine;

public class VersionProvider implements CommandLine.IVersionProvider {

    @Override
    public String[] getVersion() {
        return new String[] {
                "CDC Backfill CLI V0.0.1" ,
                "Java version: ${java.version}, ${java.vendor} (${java.vm.name} version ${java.vm.version})",
                "OS: ${os.name} version ${os.version} (${os.arch})",
                "(C) DataStax"
        };
    }
}
