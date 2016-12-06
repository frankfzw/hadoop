package org.apache.hadoop.mapreduce.scache;

import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.scache.deploy.Daemon;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    private static Daemon daemon = null;

    private static ScacheDaemon instance = null;

    protected ScacheDaemon() {
       daemon = new Daemon("hadoop");
    }

    public static ScacheDaemon getInstance() {
        if (instance == null) {
            instance = new ScacheDaemon();
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

        daemon.putBlock("hadoop", numJID, 0, numMID, reduceId, data);

    }

}
