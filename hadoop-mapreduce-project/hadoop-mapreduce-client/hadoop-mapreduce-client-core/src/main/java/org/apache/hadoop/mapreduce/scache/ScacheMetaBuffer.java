package org.apache.hadoop.mapreduce.scache;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.util.IndexedSortable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by frankfzw on 16-12-3.
 */
public class ScacheMetaBuffer implements IndexedSortable{
    private static final Log LOG = LogFactory.getLog(ScacheMetaBuffer.class.getName());
    // some static number
    private static final int VALSTART = 2;         // val offset in acct
    private static final int KEYSTART = 0;         // key offset in acct
    private static final int KEYLEN = 1;
    private static final int VALLEN = 3;           // length of value
    private static final int NMETA = 4;            // num meta ints

    public ScacheMetaBuffer(RawComparator comparator) {
        this.comparator = comparator;
    }
    private List<Integer> meta = new ArrayList<Integer>();
    private byte[] raw = null;
    private final RawComparator comparator;

    private void swapInternal(int i, int j) {
      int tmp = meta.get(i);
      meta.set(i, meta.get(j));
      meta.set(j, tmp);
    }

    public void swap(final int mi, final int mj) {
      int offi = (mi * NMETA) % meta.size();
      int offj = (mj * NMETA) % meta.size();
      for (int i = 0; i < NMETA; i ++) {
        swapInternal((offi + i), (offj + i));
      }

    }
    public int compare(final int mi, final int mj) {
      if (raw == null) {
        LOG.error("Don't apply sort before map is over!!");
        return -1;
      }
      int offi = (mi * NMETA) % meta.size();
      int offj = (mj * NMETA) % meta.size();
      int keyi = meta.get(offi);
      int keyLeni = meta.get(offi + KEYLEN);
      int keyj = meta.get(offj);
      int keyLenj = meta.get(offj + KEYLEN);
      return comparator.compare(raw, keyi, keyLeni, raw, keyj, keyLenj);

    }

    public void append(int keyStart, int keyLen, int valStart, int valLen) {
      meta.add(keyStart);
      meta.add(keyLen);
      meta.add(valStart);
      meta.add(valLen);
    }

    public void setRaw(byte[] bytes) {
      raw = bytes;
    }

    public byte[] getRaw() {
      return raw;
    }

    public List<Integer> getMeta() {
        return meta;
    }


    public int getMetaSize() {return meta.size()/NMETA;}
}
