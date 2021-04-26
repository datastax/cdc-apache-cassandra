package com.datastax.cassandra.cdc.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consume a queue of commitlog files to read mutations.
 *
 * @author vroyer
 */
@Slf4j
public class CommitLogReaderProcessor extends AbstractProcessor implements AutoCloseable {
    private static final String NAME = "CommitLogReader Processor";

    public static final String ARCHIVE_FOLDER = "archive";
    public static final String ERROR_FOLDER = "error";

    // synced position
    private AtomicReference<CommitLogPosition> syncedOffsetRef = new AtomicReference<>(new CommitLogPosition(0,0));

    private CountDownLatch syncedOffsetLatch = new CountDownLatch(1);

    private final PriorityBlockingQueue<File> commitLogQueue = new PriorityBlockingQueue<>(128, CommitLogUtil::compareCommitLogs);

    private final CommitLogReadHandlerImpl commitLogReadHandler;
    private final OffsetFileWriter offsetFileWriter;
    private final CommitLogTransfer commitLogTransfer;

    public CommitLogReaderProcessor(CommitLogReadHandlerImpl commitLogReadHandler,
                                    OffsetFileWriter offsetFileWriter,
                                    CommitLogTransfer commitLogTransfer) {
        super(NAME, 0);
        this.commitLogReadHandler = commitLogReadHandler;
        this.offsetFileWriter = offsetFileWriter;
        this.commitLogTransfer = commitLogTransfer;

        /*
        this.meterRegistry = meterRegistry;
        this.meterRegistry.gauge(MetricConstants.METRICS_PREFIX + "synced_segment", syncedOffsetRef, new ToDoubleFunction<AtomicReference<CommitLogPosition>>() {
            @Override
            public double applyAsDouble(AtomicReference<CommitLogPosition> offsetRef) {
                return offsetRef.get().segmentId;
            }
        });
        this.meterRegistry.gauge(MetricConstants.METRICS_PREFIX + "synced_position", syncedOffsetRef, new ToDoubleFunction<AtomicReference<CommitLogPosition>>() {
            @Override
            public double applyAsDouble(AtomicReference<CommitLogPosition> offsetRef) {
                return offsetRef.get().position;
            }
        });
         */
    }

    public void submitCommitLog(File file)  {
        log.debug("submitCommitLog file={}", file.getAbsolutePath());
        if (file.getName().endsWith("_cdc.idx")) {
            // you can have old _cdc.idx file, ignore it
            long seg = CommitLogUtil.extractTimestamp(file.getName());
            int pos = 0;
            if (seg >= this.syncedOffsetRef.get().segmentId) {
                try {
                    List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
                    if (lines.size() > 0) {
                        pos = Integer.parseInt(lines.get(0));
                        boolean completed = false;
                        try {
                            if("COMPLETED".equals(lines.get(1))) {
                                completed = true;
                            }
                        } catch(Exception ex) {
                        }
                        syncedOffsetRef.set(new CommitLogPosition(seg, pos));
                        String commitlogName = file.getName().substring(0, file.getName().length() - 8) + ".log";
                        log.debug("New synced position={} completed={} adding file={}", syncedOffsetRef.get(), completed, commitlogName);
                        this.commitLogQueue.add(new File(file.getParentFile(), commitlogName));

                        // unlock the processing of commitlogs
                        if(syncedOffsetLatch.getCount() > 0)
                            syncedOffsetLatch.countDown();
                    }
                } catch(IOException ex) {
                    log.warn("error while reading file=" + file.getName(), ex);
                }
            } else {
                log.debug("Ignoring old synced position from file={} pos={}", file.getName(), pos);
            }
        } else {
            this.commitLogQueue.add(file);
        }
    }

    public void awaitSyncedPosition() throws InterruptedException {
        syncedOffsetLatch.await();
    }

    @Override
    public void process() throws InterruptedException {
        assert this.offsetFileWriter.offset().segmentId <= this.syncedOffsetRef.get().segmentId || this.offsetFileWriter.offset().position <= this.offsetFileWriter.offset().position : "file offset is greater than synced offset";
        File file = null;
        while(true) {
            file = this.commitLogQueue.take();
            long seg = CommitLogUtil.extractTimestamp(file.getName());

            // ignore file before the last write offset
            if (seg < this.offsetFileWriter.offset().segmentId) {
                log.debug("Ignoring file={} before the replicated segment={}", file.getName(), this.offsetFileWriter.offset().segmentId);
                continue;
            }
            // ignore file beyond the last synced commitlog, it will be re-queued on a file modification.
            if (seg > this.syncedOffsetRef.get().segmentId) {
                log.debug("Ignore a not synced file={}, last synced offset={}", file.getName(), this.syncedOffsetRef.get());
                continue;
            }
            log.debug("processing file={} synced offset={}", file.getName(), this.syncedOffsetRef.get());
            assert seg <= this.syncedOffsetRef.get().segmentId: "reading a commitlog ahead the last synced offset";

            CommitLogReader commitLogReader = new CommitLogReader();
            try {
                // hack to use a dummy min position for segment ahead of the offsetFile.
                CommitLogPosition minPosition = (seg > offsetFileWriter.offset().segmentId)
                        ? new CommitLogPosition(seg, 0)
                        : new CommitLogPosition(offsetFileWriter.offset().getSegmentId(), offsetFileWriter.offset().getPosition());

                commitLogReader.readCommitLogSegment(commitLogReadHandler, file, minPosition, false);
                log.debug("Successfully processed commitlog immutable={} minPosition={} file={}",
                        seg < this.syncedOffsetRef.get().segmentId, minPosition, file.getName());
                if (seg < this.syncedOffsetRef.get().segmentId) {
                    commitLogTransfer.onSuccessTransfer(file);
                }
            } catch(Exception e) {
                log.warn("Failed to read commitlog immutable="+(seg < this.syncedOffsetRef.get().segmentId)+"file="+file.getName(), e);
                if (seg < this.syncedOffsetRef.get().segmentId) {
                    commitLogTransfer.onErrorTransfer(file);
                }
            }
        }
    }

    @Override
    public void initialize() throws Exception {

        File relocationDir = new File(ProducerConfig.cdcRelocationDir);

        if (!relocationDir.exists()) {
            if (!relocationDir.mkdir()) {
                throw new IOException("Failed to create " + ProducerConfig.cdcRelocationDir);
            }
        }

        File archiveDir = new File(relocationDir, ARCHIVE_FOLDER);
        if (!archiveDir.exists()) {
            if (!archiveDir.mkdir()) {
                throw new IOException("Failed to create " + archiveDir);
            }
        }
        File errorDir = new File(relocationDir, ERROR_FOLDER);
        if (!errorDir.exists()) {
            if (!errorDir.mkdir()) {
                throw new IOException("Failed to create " + errorDir);
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
