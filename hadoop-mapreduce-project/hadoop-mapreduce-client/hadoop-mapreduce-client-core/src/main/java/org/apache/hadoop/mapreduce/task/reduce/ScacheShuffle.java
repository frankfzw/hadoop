package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.reduce.ExceptionReporter;
import org.apache.hadoop.mapreduce.task.reduce.MergeManager;
import org.apache.hadoop.mapreduce.task.reduce.ShuffleClientMetrics;
import org.apache.hadoop.util.Progress;

import java.io.IOException;
import java.util.Map;

/**
 * Created by frankfzw on 17-1-10.
 */
public class ScacheShuffle<K, V> implements ShuffleConsumerPlugin<K, V>, ExceptionReporter {

    private ShuffleConsumerPlugin.Context context;

    private TaskAttemptID reduceId;
    private JobConf jobConf;
    private Reporter reporter;
    private ShuffleClientMetrics metrics;
    private TaskUmbilicalProtocol umbilical;

    private MergeManager<K, V> merger;
    private Throwable throwable = null;
    private String throwingThreadName = null;
    private Progress copyPhase;
    private TaskStatus taskStatus;
    private Task reduceTask; //Used for status updates

    @Override
    public synchronized void reportException(Throwable t) {
        if (throwable == null) {
            throwable = t;
            throwingThreadName = Thread.currentThread().getName();
            // Notify the scheduler so that the reporting thread finds the
            // exception immediately.
        }
    }

    @Override
    public void init(ShuffleConsumerPlugin.Context context) {
        this.context = context;
        this.reduceId = context.getReduceId();
        this.jobConf = context.getJobConf();
        this.umbilical = context.getUmbilical();
        this.reporter = context.getReporter();
        this.metrics = new ShuffleClientMetrics(reduceId, jobConf);
        this.copyPhase = context.getCopyPhase();
        this.taskStatus = context.getStatus();
        this.reduceTask = context.getReduceTask();

        this.merger = new MergeManagerImpl<K, V>(reduceId, jobConf, context.getLocalFS(),
                context.getLocalDirAllocator(), reporter, context.getCodec(),
                context.getCombinerClass(), context.getCombineCollector(),
                context.getSpilledRecordsCounter(),
                context.getReduceCombineInputCounter(),
                context.getMergedMapOutputsCounter(), this, context.getMergePhase(),
                context.getMapOutputFile());

    }

    @Override
    public RawKeyValueIterator run() throws IOException, InterruptedException {
        int numMapTask = jobConf.getNumMapTasks();
        ScacheFetcher<K, V>[] fetchers = new ScacheFetcher[numMapTask];
        for (int i = 0; i < numMapTask; ++i) {
            fetchers[i] = new ScacheFetcher<>(jobConf, i, reduceId, merger, reporter, metrics, this, i);
            fetchers[i].start();
        }

        //wait until all fetchers finish
        for (ScacheFetcher fetcher : fetchers) {
            fetcher.join();
            reporter.progress();
        }

        //check the exception of fetch thread
        synchronized (this) {
            if (throwable != null) {
                throw new IOException("Error in shuffle in " + throwingThreadName, throwable);
            }
        }

        copyPhase.complete();
        taskStatus.setPhase(TaskStatus.Phase.SORT);
        reduceTask.statusUpdate(umbilical);

        //merge fetch results
        RawKeyValueIterator kvIter = null;
        try {
            kvIter = merger.close();
        } catch (Throwable e) {
            throw new IOException("Error while doing final merge", e);
        }

        synchronized (this) {
            if (throwable != null) {
                throw new IOException("Error in shuffle in " + throwingThreadName, throwable);
            }
        }
        return kvIter;
    }

    @Override
    public void close() {

    }

}
