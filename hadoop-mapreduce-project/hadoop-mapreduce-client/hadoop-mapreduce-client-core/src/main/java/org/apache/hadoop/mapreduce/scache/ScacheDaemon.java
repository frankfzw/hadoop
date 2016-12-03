package org.apache.hadoop.mapreduce.scache;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.scache.deploy.Daemon;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Progress;

import java.io.IOException;
import java.util.List;

/**
 * Created by frankfzw on 16-11-29.
 */
public class ScacheDaemon {

    private static final Log LOG = LogFactory.getLog(ScacheDaemon.class.getName());

    private static Daemon daemon = new Daemon("hadoop");





}
