package org.apache.hadoop.mapreduce.scache;

import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scache.deploy.DeployMessages;
import org.scache.deploy.ReduceStatus;
import org.scache.deploy.ShuffleStatus;
import org.scache.rpc.RpcAddress;
import org.scache.rpc.RpcEndpointRef;
import org.scache.rpc.RpcEnv;
import org.scache.storage.ScacheBlockId;
import org.scache.util.ScacheConf;
import scala.reflect.ClassTag$;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by frankfzw on 16-11-29.
 */
public class ScacheDaemon {
    private static int shuffleId = 0;
    private static HashMap<Integer, List<Integer>> jobToShuffle = new HashMap<>();

    private static final Log LOG = LogFactory.getLog(ScacheDaemon.class.getName());
    private static ConcurrentHashMap<String, Long> mapSize = new ConcurrentHashMap<>();


    private static ScacheConf scacheConf = null;
    private static final Object lock = new Object();
    private static ScacheDaemon instance = null;
    private static volatile RpcEndpointRef clientRef = null;
    private static long tid = -1;
    private static ConcurrentHashMap<String, ShuffleStatus> shuffleStatus = new ConcurrentHashMap<>();
    private static HashSet<String> shuffleStatusFetchQueue = new HashSet<>();

    private static String hostname = "";
    private static String localIP = "";

    protected ScacheDaemon(final String scacheHome) {
        try {
            localIP = Inet4Address.getLocalHost().getHostAddress();
            hostname = Inet4Address.getLocalHost().getHostName();
            System.setProperty("SCACHE_DAEMON", "daemon-" + hostname);
            //LOG.info("frankfzw-debug: " + System.getenv("SCACHE_HOME"));
            scacheConf = new ScacheConf(scacheHome);
            Thread t = new Thread() {
                public void run() {
                    int clientPort = scacheConf.getInt("scache.client.port", 5678);
                    LOG.info("Start Scache Daemon of hadoop with conf: " + scacheConf.getHome() + " on " + hostname);
                    RpcEnv env = RpcEnv.create("hadoop_daemon", localIP, 12345, scacheConf, true);
                    RpcAddress clientRpcAddr = new RpcAddress(localIP, clientPort);
                    clientRef = env.setupEndpointRef(clientRpcAddr, "Client");
                    env.awaitTermination();
                }
            };
            tid = t.getId();
            t.start();
            while (clientRef == null) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    LOG.error("Init failed " + e.toString());
                }
            }
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

    public static int registerShuffle(String jobID, int numMap, int numReduce) {
        if (instance == null) {
            LOG.error("Use before INIT the ScacheDaemon \n");
            return -1;
        }
        int ret = shuffleId;
        int jid = Math.abs(jobID.hashCode());
        synchronized (jobToShuffle) {
            if (jobToShuffle.containsKey(jid)) {
                jobToShuffle.get(jid).add(shuffleId);
            } else {
                ArrayList<Integer> shuffleIds = new ArrayList<>();
                shuffleIds.add(shuffleId);
                jobToShuffle.put(jid, shuffleIds);
            }
            shuffleId ++;
        }
        // register shuffle to scache
        Boolean res = (Boolean) instance.clientRef.askWithRetry(new DeployMessages.RegisterShuffle("hadoop", jid, ret, numMap, numReduce),
                ClassTag$.MODULE$.apply(Boolean.class));
        LOG.info("Trying to register shuffle of Job " + jobID + ", get " + res.toString());

        return ret;
    }


    public static void putBlock(String jobID, int shuffleId, TaskAttemptID mapID, int reduceId, byte[] data, long rawLen, long compressedLen) {
        if (instance == null) {
            LOG.error("Use before INIT the ScacheDaemon \n");
            return;
        }
        int numJID = Math.abs(jobID.hashCode());
        int numMID = Integer.parseInt(mapID.toString().split("_")[4]);

        ScacheBlockId blockId = new ScacheBlockId("hadoop", numJID, shuffleId, numMID, reduceId);
        boolean res = false;
        LOG.debug("Start copying block " + blockId.toString() + " with size " + data.length);
        long startTime = System.currentTimeMillis();
        File f = new File(ScacheConf.scacheLocalDir() + "/" + blockId.toString());
        try {
            FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            int blockLen = 2 * Long.SIZE / Byte.SIZE + data.length;
            MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, blockLen);
            buf.putLong(rawLen);
            buf.putLong(compressedLen);
            buf.put(data);
            res = (Boolean) instance.clientRef.askWithRetry(new DeployMessages.PutBlock(blockId, blockLen),
                    ClassTag$.MODULE$.apply(Boolean.class));
        } catch (IOException e) {
            LOG.error("File: " + f.toPath().toString() + " not found");
        }
        long endTime = System.currentTimeMillis();
        if (res) {
            LOG.debug("Copy block " + blockId.toString() + " to Scache successfully in " + (endTime - startTime) + " ms");
        } else {
            LOG.error("Copy block " + blockId.toString() + " to Scache failed in " + (endTime - startTime) + " ms");
        }


    }

    public static BlockFromScache getBlock(String jobId, int shuffleId, int mapId, int reduceId) {
        if (instance == null) {
            LOG.error("Use before INIT the ScacheDaemon \n");
            return null;
        }
        int numJID = Math.abs(jobId.hashCode());
        ScacheBlockId blockId = new ScacheBlockId("hadoop", numJID, shuffleId, mapId, reduceId);
        try {
            HashMap<Integer, List<String>> shuffleStatus = getShuffleStatus(jobId, shuffleId);
            if (!shuffleStatus.get(reduceId).contains(localIP)) {
                String hosts = "";
                for (String h : shuffleStatus.get(reduceId)) {
                    hosts = hosts + " " + h;
                }
                LOG.error("Host "+ localIP + " Receive wrong block fetch request from " + blockId.toString()
                + " reduce should run on " + hosts);
                return null;
            }
        } catch (Exception e) {
            LOG.error("Fetch shuffle statuses wrong in getBlock");
            return null;
        }

        long startTime = System.currentTimeMillis();
        int size = (Integer) instance.clientRef.askWithRetry(new DeployMessages.GetBlock(blockId), ClassTag$.MODULE$.apply(Integer.class));
        if (size < 0) {
            LOG.error("Can't get block " + blockId.toString());
            return null;
        }
        LOG.debug("Start fetching block " + blockId.toString() + " with size " + size);
        File f = new File(ScacheConf.scacheLocalDir() + "/" + blockId.toString());
        long rawSize = 0L;
        long compressedSize = 0L;
        try {
            FileChannel channel = FileChannel.open(f.toPath(),
                    StandardOpenOption.READ, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
            MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            rawSize = buf.getLong();
            compressedSize = buf.getLong();
            byte[] bytes = new byte[size - 2 * Long.SIZE / Byte.SIZE];
            buf.get(bytes);
            channel.close();
            long endTime = System.currentTimeMillis();
            LOG.debug("Copy block " + blockId.toString() + " from Scache in " + (endTime - startTime) + " ms");
            BlockFromScache block = new BlockFromScache(rawSize, compressedSize, bytes);
            return block;
        } catch (IOException e) {
            LOG.error("File: " + f.toPath().toString() + " not found");
            return null;
        }

    }

    public static void updateMapSize(TaskAttemptID mapId, long size) {
        if (instance == null) {
            LOG.error("Use before INIT the ScacheDaemon \n");
            return;
        }
        instance.mapSize.putIfAbsent(mapId.toString(), size);
    }

    public static HashMap<Integer, List<String>> getShuffleStatus(String jobID, int shuffleId) throws Exception{
        if (instance == null) {
            LOG.error("Use before INIT the ScacheDaemon \n");
            throw new Exception("Use before INIT the ScacheDaemon \n");
        }
        int numJID = Math.abs(jobID.hashCode());
        HashMap<Integer, List<String>> ret = new HashMap<>();

        String shuffleIndex = Integer.toString(numJID) + "_" + Integer.toString(shuffleId);

        ShuffleStatus res;
        if (instance.shuffleStatus.containsKey(shuffleIndex)) {
            res = instance.shuffleStatus.get(shuffleIndex);
        } else {
            synchronized (instance.shuffleStatusFetchQueue) {
                if (instance.shuffleStatusFetchQueue.contains(shuffleIndex)) {
                    while (!instance.shuffleStatus.containsKey(shuffleIndex)) {
                        instance.shuffleStatusFetchQueue.wait();
                    }
                } else {
                    if (!instance.shuffleStatus.containsKey(shuffleIndex)) {
                        instance.shuffleStatusFetchQueue.add(shuffleIndex);
                    }
                }
            }
            if (instance.shuffleStatus.containsKey(shuffleIndex)) {
                res = instance.shuffleStatus.get(shuffleIndex);
            } else {
                res = (ShuffleStatus) instance.clientRef.askWithRetry(new DeployMessages.GetShuffleStatus("hadoop", numJID, shuffleId),
                        ClassTag$.MODULE$.apply(ShuffleStatus.class));
                instance.shuffleStatus.putIfAbsent(shuffleIndex, res);
                synchronized (instance.shuffleStatusFetchQueue) {
                    instance.shuffleStatusFetchQueue.remove(shuffleIndex);
                    instance.shuffleStatusFetchQueue.notifyAll();
                }
            }
        }

        for (ReduceStatus rs : res.reduceArray()) {
            List<String> tmp = new ArrayList<>();
            tmp.add(rs.host());
            for (String backup : rs.backups()) {
                tmp.add(backup);
            }
            ret.put(rs.id(), tmp);
        }
        return ret;
    }

    public static long getMapSize(TaskAttemptID tId) {
        if (instance == null) {
            LOG.error("Use before INIT the ScacheDaemon \n");
            return -1;
        }
        return instance.mapSize.get(tId.toString());
    }

    public static void mapEnd(String jobId, int shuffleId, TaskAttemptID mapId) {
        if (instance == null) {
            LOG.error("Use before INIT the ScacheDaemon \n");
            return;
        }
        int numJID = Math.abs(jobId.hashCode());
        int numMID = Integer.parseInt(mapId.toString().split("_")[4]);
        instance.clientRef.send(new DeployMessages.MapEnd("hadoop", numJID, shuffleId, numMID));
    }
    public static void putEmptyBlock(String jobID, int shuffleId, TaskAttemptID mapID, int reduceId) {
        if (instance == null) {
            LOG.error("Use before INIT the ScacheDaemon \n");
            return;
        }
        int numJID = Math.abs(jobID.hashCode());
        int numMID = Integer.parseInt(mapID.toString().split("_")[4]);
        ScacheBlockId blockId = new ScacheBlockId("hadoop", numJID, shuffleId, numMID, reduceId);
        instance.clientRef.send(new DeployMessages.PutBlock(blockId, 0));

    }

}
