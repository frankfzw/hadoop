package org.apache.hadoop.mapreduce.util;

import org.scache.deploy.Daemon;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Created by frankfzw on 16-11-29.
 */
public class ScacheDaemon {

    private static final Log LOG = LogFactory.getLog(ScacheDaemon.class.getName());

    private static Daemon daemon = new Daemon("hadoop");
}
