package org.apache.hadoop.mapreduce.scache;

/**
 * Created by frankfzw on 17-1-13.
 */
public class BlockFromScache {
    public long rawSize;
    public long compressedSize;
    public byte[] buf;

    public BlockFromScache(long rawSize, long compressedSize, byte[] buf) {
        this.rawSize = rawSize;
        this.compressedSize = compressedSize;
        this.buf = buf.clone();
    }
}
