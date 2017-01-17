package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.scache.BlockFromScache;
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

    private static final MapHost LOCALHOST = new MapHost("local", "local");

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
        BlockFromScache sBlock = ScacheDaemon.getInstance().getBlock(reduceId.getJobID().toString(),
                conf.getInt(MRJobConfig.SCACHE_SHUFFLE_ID, 0), numMapId, numReduceId);
        if (sBlock == null) {
            LOG.error("Can't fetch reduce block " + reduceId.getJobID().toString() + "_" + numMapId + "_" + numReduceId + " from SCache");
            exceptionReporter.reportException(new Throwable("Can't fetch reduce block " + reduceId.getJobID().toString() + "_" + numMapId + "_" + numReduceId + " from SCache"));
            return;
        }
        MapOutput<K, V> mapOutput = null;
        try {
            long compressedLength = sBlock.compressedSize;
            long decompressedLength = sBlock.rawSize;

            compressedLength -= CryptoUtils.cryptoPadding(conf);
            decompressedLength -= CryptoUtils.cryptoPadding(conf);
            merger.waitForResource();
            ByteArrayInputStream bos = new ByteArrayInputStream(sBlock.buf);
            InputStream input = new DataInputStream(bos);
            input = CryptoUtils.wrapIfNecessary(conf, input, compressedLength);
            mapOutput = merger.reserve(mapId, decompressedLength, id);
            if (mapOutput == null) {
                LOG.error("fetcher#" + id + "- MergerManager is not available");
                return;
            }
            mapOutput.shuffle(LOCALHOST, input, compressedLength, decompressedLength, metrics, reporter);
            metrics.successFetch();
        } catch (InterruptedException ie) {
            return;
        } catch (Throwable t) {
            metrics.failedFetch();
            exceptionReporter.reportException(t);
        }

    }
}
