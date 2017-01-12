package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.scache.ScacheDaemon;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;

/**
 * Created by frankfzw on 17-1-10.
 */
public class ScacheFetcher<K, V> extends Thread {
    private static final Log LOG = LogFactory.getLog(Fetcher.class);

    private JobConf conf;
    private TaskAttemptID mapId;
    private TaskAttemptID reduceId;
    private MergeManager merger;
    private Reporter reporter;
    private ShuffleClientMetrics metrics;
    private ExceptionReporter exceptionReporter;
    private int id;

    public ScacheFetcher(JobConf conf, TaskAttemptID mapId, TaskAttemptID reduceId,
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
        int numReduceId = Integer.parseInt(reduceId.toString().split("_")[4]);
        int numMapId = Integer.parseInt(mapId.toString().split("_")[4]);
        byte[] bytes = ScacheDaemon.getInstance().getBlock(reduceId.getJobID().toString(),
                conf.getInt(MRJobConfig.SCACHE_SHUFFLE_ID, 0), numMapId, numReduceId);
        if (bytes == null) {
            LOG.error("Can't fetch reduce block " + reduceId.getJobID().toString() + ":" + numMapId + ":" + numReduceId + " from SCache");
            exceptionReporter.reportException(new Throwable("Can't fetch reduce block " + reduceId.getJobID().toString() + ":" + mapId + ":" + numReduceId + " from SCache"));
            return;
        }
        MapOutput<K, V> mapOutput = null;
        try {
            int length = bytes.length;
            merger.waitForResource();
            ByteArrayInputStream bos = new ByteArrayInputStream(bytes);
            InputStream input = new DataInputStream(bos);
            input = CryptoUtils.wrapIfNecessary(conf, input, length);
            length -= CryptoUtils.cryptoPadding(conf);
            mapOutput = merger.reserve(mapId, bytes.length, id);
            if (mapOutput == null) {
                LOG.error("fetcher#" + id + "- MergerManager is not available");
                return;
            }
            MapHost localHost = new MapHost("localhost", "localhost");
            mapOutput.shuffle(localHost, input, length, length, metrics, reporter);
            metrics.successFetch();
        } catch (InterruptedException ie) {
            return;
        } catch (Throwable t) {
            metrics.failedFetch();
            exceptionReporter.reportException(t);
        }

    }
}
