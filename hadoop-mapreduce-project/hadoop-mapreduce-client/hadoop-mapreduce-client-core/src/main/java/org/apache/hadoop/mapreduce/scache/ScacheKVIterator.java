package org.apache.hadoop.mapreduce.scache;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.util.Progress;

import java.io.IOException;
import java.util.List;

/**
 * Created by frankfzw on 16-12-3.
 */
public class ScacheKVIterator implements RawKeyValueIterator {
    // some static number
    private static final int VALSTART = 2;         // val offset in acct
    private static final int KEYSTART = 0;         // key offset in acct
    private static final int KEYLEN = 1;
    private static final int VALLEN = 3;           // length of value
    private static final int NMETA = 4;            // num meta ints

    private final DataInputBuffer keybuf = new DataInputBuffer();
    private final DataInputBuffer vbytes = new DataInputBuffer();
    private final int end;
    private int current;
    private byte[] rawData;
    private List<Integer> kvmeta;

    public ScacheKVIterator(ScacheMetaBuffer buf) {
        this.kvmeta = buf.getMeta();
        this.rawData = buf.getRaw();
        this.end = buf.getMetaSize();
        current = -1;
    }
    public boolean next() throws IOException {
        return ++current < end;
    }
    public DataInputBuffer getKey() throws IOException {
        final int kvoff = (current * NMETA) % kvmeta.size();
        keybuf.reset(rawData, kvmeta.get(kvoff + KEYSTART), kvmeta.get(kvoff + KEYLEN));
        return keybuf;
    }
    public DataInputBuffer getValue() throws IOException {
        final int kvoff = (current * NMETA) % kvmeta.size();
        vbytes.reset(rawData, kvmeta.get(kvoff + VALSTART), kvmeta.get(kvoff + VALLEN));
        return vbytes;
    }
    public Progress getProgress() {
        return null;
    }
    public void close() { }
}
