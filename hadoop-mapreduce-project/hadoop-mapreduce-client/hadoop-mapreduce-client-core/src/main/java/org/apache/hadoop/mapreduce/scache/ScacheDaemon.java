package org.apache.hadoop.mapreduce.scache;

import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scache.deploy.DeployMessages;
import org.scache.rpc.RpcAddress;
import org.scache.rpc.RpcEndpointRef;
import org.scache.rpc.RpcEnv;
import org.scache.storage.ScacheBlockId;
import org.scache.util.ScacheConf;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;


/**
 * Created by frankfzw on 16-11-29.
 */
public class ScacheDaemon {
    private static int shuffleId = 0;
    private static int jobId = 0;
    private static HashMap<JobID, Integer> jobToId = new HashMap<>();
    private static HashMap<Integer, List<Integer>> jobToShuffle = new HashMap<>();

    private static final Log LOG = LogFactory.getLog(ScacheDaemon.class.getName());


    private static ScacheConf scacheConf = null;
    private static final Object lock = new Object();
    private static ScacheDaemon instance = null;
    private static RpcEndpointRef clientRef = null;
    private static long tid = -1;

    protected ScacheDaemon(String scacheHome) {
        try {
            final String localIP = Inet4Address.getLocalHost().getHostAddress();
            //LOG.info("frankfzw-debug: " + System.getenv("SCACHE_HOME"));
            scacheConf = new ScacheConf(scacheHome);
            Thread t = new Thread() {
                public void run() {
                    LOG.info("Start Scache Daemon of hadoop with conf: " + scacheConf.scacheHome() + " on " + localIP);
                    RpcEnv env = RpcEnv.create("hadoop_daemon", localIP, 12345, scacheConf, true);
                    RpcAddress clientRpcAddr = new RpcAddress(localIP, 5678);
                    clientRef = env.setupEndpointRef(clientRpcAddr, "Client");
                    env.awaitTermination();
                }
            };
            tid = t.getId();
            t.start();
        } catch (UnknownHostException e) {
            LOG.error("Address not found\n");
        }


    }

    public static ScacheDaemon getInstance() {
        if (instance != null) {
            return instance;
        }
        LOG.error("ScacheDaemon use before init");
        return null;
    }

    public static ScacheDaemon initInstance(String scacheHome) {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new ScacheDaemon(scacheHome);
                    LOG.info("frankfzw-debug: home dir is " + scacheHome);
                }
            }
        }
        return instance;
    }

    public static int registerShuffle(JobID jobID) {
        int ret = shuffleId;
        synchronized (jobToId) {
            if (!jobToId.containsKey(jobID)) {
                jobToId.put(jobID, jobId);
                jobId ++;
            }
        }
        // synchronized (jobToShuffle){
        //     if (!jobToShuffle.containsKey(numId)) {
        //         jobToShuffle.put(numId, new ArrayList<Integer>());
        //     }
        //     jobToShuffle.get(numId).add(shuffleId);
        //     shuffleId ++;
        // }
        return ret;
    }


    public static void putBlock(JobID jobID, int shuffleId, TaskAttemptID mapID, int reduceId, byte[] data) {
        int numJID = jobToId.get(jobID);
        int numMID = Integer.parseInt(mapID.toString().split("_")[4]);

        ScacheBlockId blockId = new ScacheBlockId("hadoop", numJID, shuffleId, numMID, reduceId);
        LOG.debug("Start copying block " + blockId.toString() + " with size " + data.length);
        long startTime = System.currentTimeMillis();
        File f = new File(ScacheConf.scacheLocalDir() + "/" + blockId.toString());
        try {
            FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, data.length);
            buf.put(data, 0, data.length);
            clientRef.send(new DeployMessages.PutBlock(blockId, data.length));
        } catch (IOException e) {
            LOG.error("File: " + f.toPath().toString() + " not found");
        }
        long endTime = System.currentTimeMillis();
        LOG.debug("Copy block " + blockId.toString() + " to Scache in " + (endTime - startTime) + " ms");

    }

}
