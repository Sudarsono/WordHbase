
package com.aldy.wordhbase.crud;

import com.aldy.wordhbase.data.Book;
import com.aldy.wordhbase.db.DbHelper;
import com.aldy.wordhbase.db.Scema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author sudarsono
 */
public class CrudBook {

    private DbHelper dbHelper;

    public CrudBook() throws IOException {
        this.dbHelper = DbHelper.getInstance();
    }

    public void delete(String rowKey) throws IOException {
        dbHelper.deleteEntireRow(rowKey, Scema.BOOK_TABLE, Scema.BOOK_CF_DATA);
    }

    public String insert(Book book) throws IOException {
        try (Table tbl = dbHelper.getConnection().getTable(
                TableName.valueOf(Scema.BOOK_TABLE))) {
            long rowKey = Long.MAX_VALUE - tbl.incrementColumnValue(Scema.ROW_KEY_ID_COUNTER,
                    Scema.CF_COUNTER, Scema.CQ_COUNTER, 1l);
            book.setId(String.format("%019d", rowKey));
            Put put = new Put(Bytes.toBytes(book.getId()));
            book.setId(String.valueOf(rowKey));
            put.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_ISBN, Bytes.toBytes(book.getIsbn()));
            put.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_TITLE, Bytes.toBytes(book.getTitle()));
            put.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_CONTENT, Bytes.toBytes(book.getContent()));
            tbl.put(put);
        }
        return book.getId();
    }

    public String update(Book book) throws IOException {
        try (Table tbl = dbHelper.getConnection().getTable(
                TableName.valueOf(Scema.BOOK_TABLE))) {
            Put put = new Put(Bytes.toBytes(book.getId()));
            if (book.getIsbn()!= null) {
                put.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_ISBN, Bytes.toBytes(book.getIsbn()));
            }
            if (book.getTitle()!= null) {
                put.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_TITLE, Bytes.toBytes(book.getTitle()));
            }
            if (book.getContent()!= null) {
                put.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_CONTENT, Bytes.toBytes(book.getContent()));
            }
            tbl.put(put);
        }
        return book.getId();
    }

    public Book select(String bookId)
            throws IOException {
        Book booklication;
        try (Table table = dbHelper.getConnection().getTable(TableName.valueOf(
                Scema.BOOK_TABLE))) {
            booklication = null;
            if (bookId != null) {
                Get get = createGetter(bookId);
                Result result = table.get(get);
                if (!result.isEmpty()) {
                    booklication = resultToBook(result);
                }
            }
        }
        return booklication;
    }

    public List<Book> select(String startRow, int pageSize, String isbn,
            String title, String content) throws IOException {
        Table table = dbHelper.getConnection().getTable(TableName.valueOf(
                Scema.BOOK_TABLE));
        List<Book> booklications = new ArrayList<>();

        Scan scan = createScanner();
        if (startRow != null && startRow.length() > 0) {
            scan.setStartRow(startRow.getBytes());
        }
        List<Filter> filters = new ArrayList<>();
        if (isbn != null) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_ISBN,
                    CompareFilter.CompareOp.GREATER_OR_EQUAL, new SubstringComparator(isbn));
            filters.add(filter);
        }
        if (title != null) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_TITLE,
                    CompareFilter.CompareOp.GREATER_OR_EQUAL, new SubstringComparator(title));
            filters.add(filter);
        }
        if (content != null) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_CONTENT,
                    CompareFilter.CompareOp.GREATER_OR_EQUAL, new SubstringComparator(content));
            filters.add(filter);
        }
        Filter pageFilter = new PageFilter(pageSize);
        filters.add(pageFilter);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            booklications.add(resultToBook(result));
        }
        if (startRow != null && startRow.length() > 0 && booklications.size() > 0) {
            booklications.remove(0);
        }
        return booklications;
    }

    private Book resultToBook(Result result) {
        Book book = new Book();
        book.setId(Bytes.toString(result.getRow()));
        book.setIsbn(Bytes.toString(result.getValue(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_ISBN)));
        book.setTitle(Bytes.toString(result.getValue(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_TITLE)));
        book.setContent(Bytes.toString(result.getValue(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_CONTENT)));
        return book;
    }

    private Scan createScanner() {
        Scan scan = new Scan();
        scan.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_ISBN);
        scan.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_TITLE);
        scan.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_CONTENT);
        return scan;
    }

    private Get createGetter(String rowKey) {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_ISBN);
        get.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_TITLE);
        get.addColumn(Scema.BOOK_CF_DATA, Scema.BOOK_CQ_CONTENT);
        return get;
    }
}
