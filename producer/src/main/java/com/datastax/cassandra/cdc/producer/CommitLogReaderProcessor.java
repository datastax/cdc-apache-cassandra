package com.datastax.cassandra.cdc.producer;

import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.*;

/**
 * Manage BlockingCommitLogReaders to read synced data as soon as possible.
 *
 */
@Singleton
public class CommitLogReaderProcessor extends AbstractProcessor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReaderProcessor.class);
    private static final String NAME = "CommitLogReader Processor";

    // synced position
    private volatile long syncedSegmentId = -1;
    private volatile int  syncedPosition = -1;
    private CountDownLatch syncedPositionLatch = new CountDownLatch(1);

    private final PriorityBlockingQueue<File> commitLogQueue = new PriorityBlockingQueue<>(128, CommitLogUtil::compareCommitLogs);

    private final MutationQueue changeEventQueue;
    private final CommitLogReadHandlerImpl commitLogReadHandler;
    private final FileOffsetWriter fileOffsetWriter;
    private final CommitLogTransfer commitLogTransfer;

    public CommitLogReaderProcessor(CassandraConnectorConfiguration config,
                                    CommitLogReadHandlerImpl commitLogReadHandler,
                                    MutationQueue changeEventQueue,
                                    FileOffsetWriter fileOffsetWriter,
                                    CommitLogTransfer commitLogTransfer) {
        super(NAME, 0);
        this.changeEventQueue = changeEventQueue;
        this.commitLogReadHandler = commitLogReadHandler;
        this.fileOffsetWriter = fileOffsetWriter;
        this.commitLogTransfer = commitLogTransfer;
    }

    public void submitCommitLog(File file)  {
        if (file.getName().endsWith("_cdc.idx")) {
            // you can have old _cdc.idx file, ignore it
            long seg = CommitLogUtil.extractTimestamp(file.getName());
            int pos = 0;
            if (seg >= this.syncedSegmentId) {
                try {
                    List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
                    pos = Integer.parseInt(lines.get(0));
                    boolean completed = false;
                    try {
                        if("COMPLETED".equals(lines.get(1))) {
                            completed = true;
                        }
                    } catch(Exception ex) {
                    }
                    logger.debug("New synced position={}:{} completed={}", seg, pos, completed);
                    assert seg > this.syncedSegmentId || pos > this.syncedPosition : "Unexpected synced position " + seg + ":" +pos;

                    this.syncedSegmentId = seg;
                    this.syncedPosition = pos;
                    // unlock the processing of commitlogs
                    if (syncedPositionLatch.getCount() > 0)
                        syncedPositionLatch.countDown();
                } catch(IOException ex) {
                    logger.warn("error while reading file=" + file.getName(), ex);
                }
            } else {
                logger.debug("Ignoring old synced position from file={} pos={}", file.getName(), pos);
            }
        } else {
            this.commitLogQueue.add(file);
        }
    }

    public void awaitSyncedPosition() throws InterruptedException {
        syncedPositionLatch.await();
    }

    @Override
    public void process() throws InterruptedException {
        assert this.syncedSegmentId >= this.fileOffsetWriter.position().segmentId : "offset segment is beyond the synced segment";
        assert this.syncedSegmentId > this.fileOffsetWriter.position().segmentId || this.syncedPosition > this.fileOffsetWriter.position().position : "offset is beyond the synced position";
        File file = null;
        while(true) {
            file = this.commitLogQueue.take();
            long seg = CommitLogUtil.extractTimestamp(file.getName());

            // ignore file before the last write offset
            if (seg < this.fileOffsetWriter.position().segmentId) {
                logger.debug("Ignoring file={} before the replicated segment={}", file.getName(), this.fileOffsetWriter.position().segmentId);
                continue;
            }
            // ignore file beyond the last synced commitlog, it will be re-queued on a file modification.
            if (seg > this.syncedSegmentId) {
                logger.debug("Ignore dummy file={} after the synced segment={}", file.getName(), this.syncedSegmentId);
                continue;
            }
            logger.debug("processing file={} synced position={}:{}",
                    file.getName(), this.syncedSegmentId, this.syncedPosition);
            assert seg <= this.syncedSegmentId : "reading a commitlog ahead the last synced commitlog";

            CommitLogReader commitLogReader = new CommitLogReader();
            try {
                commitLogReader.readCommitLogSegment(commitLogReadHandler, file, fileOffsetWriter.position(), false);
                logger.debug("Successfully processed commitlog immutable={} file={}", seg < this.syncedSegmentId, file.getName());
                if (seg < this.syncedSegmentId) {
                    commitLogTransfer.onSuccessTransfer(file);
                }
            } catch(Exception e) {
                logger.warn("Failed to read commitlog immutable="+(seg < this.syncedSegmentId)+"file="+file.getName(), e);
                if (seg < this.syncedSegmentId) {
                    commitLogTransfer.onErrorTransfer(file);
                }
            }
        }
    }

    /**
     * Override destroy to clean up resources after stopping the processor
     */
    @Override
    public void close() {
    }
}
