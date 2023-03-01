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

// copy of DSBulk ExitStatus
public enum ExitStatus {
    STATUS_OK(0),
    STATUS_COMPLETED_WITH_ERRORS(1),
    STATUS_ABORTED_TOO_MANY_ERRORS(2),
    STATUS_ABORTED_FATAL_ERROR(3),
    STATUS_INTERRUPTED(4),
    STATUS_CRASHED(5),
    ;

    private final int exitCode;

    ExitStatus(int exitCode) {
        this.exitCode = exitCode;
    }

    public static ExitStatus forCode(int exitCode) {
        switch (exitCode) {
            case 0:
                return ExitStatus.STATUS_OK;
            case 1:
                return ExitStatus.STATUS_COMPLETED_WITH_ERRORS;
            case 2:
                return ExitStatus.STATUS_ABORTED_TOO_MANY_ERRORS;
            case 3:
                return ExitStatus.STATUS_ABORTED_FATAL_ERROR;
            case 4:
                return ExitStatus.STATUS_INTERRUPTED;
            default:
                return ExitStatus.STATUS_CRASHED;
        }
    }

    public int exitCode() {
        return exitCode;
    }
}
