
package com.aldy.wordhbase.db;

import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author sudarsono
 */
public class Scema {

    protected static Scema instance;

    private Scema() {
    }

    public static Scema getInstance() {
        if (instance == null) {
            instance = new Scema();
        }
        return instance;
    }
    
    public final static byte[] CF_COUNTER = Bytes.toBytes("counter");
    public final static byte[] CQ_COUNTER = Bytes.toBytes("counter");
    public final static byte[] ROW_KEY_ID_COUNTER = Bytes.toBytes("id");
    // 6. table:voucher
    public final static byte[] BOOK_TABLE = Bytes.toBytes("book");
    public final static byte[] BOOK_CF_DATA = Bytes.toBytes("d");
    // 6.1 table:voucher, cf:data
    public final static byte[] BOOK_CQ_ISBN = Bytes.toBytes("isbn");
    public final static byte[] BOOK_CQ_TITLE = Bytes.toBytes("title");
    public final static byte[] BOOK_CQ_CONTENT = Bytes.toBytes("content");
    
}
