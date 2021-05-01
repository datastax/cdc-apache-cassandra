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

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.cassandra.cdc.producer.exceptions.CassandraConnectorDataException;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for commit log files.
 */
@Slf4j
public final class CommitLogUtil {
    public static final Pattern FILENAME_REGEX_PATTERN = Pattern.compile("CommitLog-\\d+-(\\d+)(\\.log|_cdc\\.idx)");

    private CommitLogUtil() {
    }

    /**
     * Move a commit log to a new directory. If the commit log already exists in the new directory, it woull be replaced.
     */
    public static void moveCommitLog(File file, Path toDir) {
        try {
            /* TODO: fails on .idx files
            Matcher filenameMatcher = FILENAME_REGEX_PATTERN.matcher(file.getName());
            if (!filenameMatcher.matches()) {
                throw new IllegalArgumentException("Cannot move file because " + file.getName() + " does not appear to be a CommitLog");
            }
            */
            Files.move(file.toPath(), toDir.resolve(file.getName()), REPLACE_EXISTING);
        }
        catch (Exception e) {
            log.error("Failed to move the file {} from {}", file.getName(), toDir.getFileName(), e);
        }
    }

    /**
     * Delete a commit log and logs the error in the case the deletion failed.
     */
    public static void deleteCommitLog(File file) {
        try {
            Matcher filenameMatcher = FILENAME_REGEX_PATTERN.matcher(file.getName());
            if (!filenameMatcher.matches()) {
                throw new IllegalArgumentException("Cannot delete file because " + file.getName() + " does not appear to be a CommitLog");
            }

            Files.delete(file.toPath());
            log.debug("Deleted commit log {} in cdc directory", file.getName());
        }
        catch (Exception e) {
            log.error("Failed to delete the file {} from cdc directory: ", file.getName(), e);
        }
    }

    /**
     * Given a directory, return an array of commit logs in this directory.
     * If the directory does not contain any commit logs, an empty array is returned.
     */
    public static File[] getCommitLogs(File directory) {
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("Given directory does not exist: " + directory);
        }

        return directory.listFiles(f -> f.isFile() && FILENAME_REGEX_PATTERN.matcher(f.getName()).matches());
    }

    /**
     * Comparing two commit log files provided the {@link File} instances;
     * Returns 0 if they are the same, -1 if first file is older, 1 if first file is newer.
     */
    public static int compareCommitLogs(File file1, File file2) {
        long ts1 = extractTimestamp(file1.getName());
        long ts2 = extractTimestamp(file2.getName());
        return Long.compare(ts1, ts2);
    }

    /**
     * Comparing two commit log files provided the file names.
     * Returns 0 if they are the same, -1 if first file is older, 1 if first file is newer.
     */
    public static int compareCommitLogs(String filename1, String filename2) {
        if (filename1.equals(filename2)) {
            return 0;
        }
        long ts1 = extractTimestamp(filename1);
        long ts2 = extractTimestamp(filename2);
        return Long.compare(ts1, ts2);
    }

    public static long extractTimestamp(String commitLogFileName) {
        Matcher filenameMatcher = FILENAME_REGEX_PATTERN.matcher(commitLogFileName);
        if (!filenameMatcher.matches()) {
            throw new CassandraConnectorDataException("Cannot extract timestamp because " + commitLogFileName + " does not appear to be a CommitLog");
        }
        return Long.parseLong(filenameMatcher.group(1));
    }
}
