/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aldy.wordhbase.db;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author sudarsono
 */
public class DbHelper {

    private Configuration conf;
    private Connection connection;
    private Admin admin;
    private static final int maxVersion = 1;
    private static DbHelper instance = null;

    private DbHelper() throws IOException {
        this.conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
    }

    public static DbHelper getInstance() throws IOException {
        if (instance == null) {
            instance = new DbHelper();
        }
        return instance;
    }

    public Connection getConnection() {
        return connection;
    }

    private void createTable(byte[] tableName, byte[][] colFams, boolean createIncrement) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(
                TableName.valueOf(tableName));
        for (byte[] cf : colFams) {
            HColumnDescriptor coldef = new HColumnDescriptor(cf);
            coldef.setMaxVersions(maxVersion);
            desc.addFamily(coldef);
        }
        admin.createTable(desc);
        if (createIncrement) {
            createIncrement(tableName);
        }
    }

    private void createIncrement(byte[] tableName) throws IOException {
        try (Table tbl = DbHelper.getInstance().getConnection().getTable(
                     TableName.valueOf(tableName))) {
            Put put = new Put(Scema.ROW_KEY_ID_COUNTER);
            put.addColumn(Scema.CF_COUNTER, Scema.CQ_COUNTER, Bytes.toBytes(0l));
            tbl.put(put);
        }
    }

    public void createTables() throws IOException {
        createTable(Scema.BOOK_TABLE, new byte[][]{
            Scema.BOOK_CF_DATA, Scema.CF_COUNTER}, true);
    }

    public void dropTables() throws IOException {
        dropTable(Scema.BOOK_TABLE);
    }

    public void dropTable(byte[] tableName) throws IOException {
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            if (admin.isTableEnabled(table)) {
                admin.disableTable(table);
            }
            admin.deleteTable(table);
        }
    }

    public void put(TableName table, String row, String fam, String qual,
            String val) throws IOException {
        try (Table tbl = connection.getTable(table)) {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), Bytes.toBytes(val));
            tbl.put(put);
        }
    }

    public void addValueFilter(List<Filter> filters, byte[] clmFm, byte[] clmQlf, String comparator) {
        if (comparator != null && !comparator.equals("0")) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    clmFm, clmQlf,
                    CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(comparator));
            filters.add(filter);
        }
    }

    public void deleteColumnFamily(String rowKey, byte[] table, byte[] clmFm) throws IOException {
        try (Table tbl = getConnection().getTable(
                     TableName.valueOf(table))) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addFamily(clmFm);
            tbl.delete(delete);
        }
    }

    public void deleteEntireRow(String rowKey, byte[] table, byte[] clmFm) throws IOException {
        try (Table tbl = getConnection().getTable(
                     TableName.valueOf(table))) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            tbl.delete(delete);
        }
    }
}
