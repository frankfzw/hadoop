package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Created by frankfzw on 17-1-10.
 */
public class ScacheFetcher<K, V> extends Thread {
    private static final Log LOG = LogFactory.getLog(Fetcher.class);

    private JobConf conf;
    private int mapId;
    private TaskAttemptID reduceId;
    private MergeManager merger;
    private Reporter reporter;
    private ShuffleClientMetrics metrics;
    private ExceptionReporter exceptionReporter;
    private int id;

    public ScacheFetcher(JobConf conf, int mapId, TaskAttemptID reduceId,
                         MergeManager<K, V> merger, Reporter reporter, ShuffleClientMetrics metrics,
                         ExceptionReporter exceptionReporter, int id) {
        this.conf = conf;
        this.mapId = mapId;
        this.reduceId = reduceId;
        this.merger = merger;
        this.reporter = reporter;
        this.metrics = metrics;
        this.exceptionReporter = exceptionReporter;
        this.id = id;

        setName("fetcher#" + id);
    }

    public void run() {
        //TODO fetch data from scache
    }
}
