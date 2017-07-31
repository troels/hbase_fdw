package org.bifrost;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.bifrost.utils.ArrayUtils;
import org.bifrost.utils.PairStore;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class HBaseToPgScanner implements Scanner, AutoCloseable {
    private final ResultScanner scanner;
    private final PgHbaseColumn[] columns;
    private final Table table;
    private Result nextResult;

    HBaseToPgScanner(final Table table,
                     final ResultScanner scanner,
                     PgHbaseColumn[] columns) {
        this.scanner = scanner;
        this.columns = columns;
        this.table = table;
    }

    @Override
    public boolean scan(ByteBuffer buf) throws IOException {
        if (scanner == null) return false;
        buf.order(ByteOrder.nativeOrder());
        if (nextResult == null) {
            nextResult = scanner.next();
        }
        if (nextResult == null) {
            return false;
        }
        serializeResult(buf, nextResult);
        nextResult = null;
        return true;
    }

    @Override
    public byte[] scan() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(65536);
        buf.order(ByteOrder.nativeOrder());
        boolean res = scan(buf);
        if (!res) return null;
        return buf.array();
    }


    private void serializeResult(ByteBuffer buf, Result result) throws UnsupportedEncodingException {
        int startPos = buf.position();
        buf.putInt(0);
        for (PgHbaseColumn column: columns) {
            int lenPos = buf.position();
            buf.putInt(0);
            writeColumns(buf, column, result);
            PgDatum.align(buf, 4);
            int endPos = buf.position();
            buf.position(lenPos);
            buf.putInt(endPos - lenPos);
            buf.position(endPos);
        }
        buf.putInt(0);
        int endPos = buf.position();
        buf.position(startPos);
        buf.putInt(endPos - startPos);
        buf.position(endPos);
    }

    private void writeColumns(ByteBuffer buf, PgHbaseColumn column, Result result) throws UnsupportedEncodingException {
        Cell[] cells = result.rawCells();

        if (column.row) {
            PgDatum.writeByteArrayDatum(buf, cells[0].getRowArray(), cells[0].getRowOffset(), cells[0].getRowLength());
        } else if (column.family) {
            List<Cell> familyCells = new ArrayList<>(cells.length);
            for (int i = 0; i < cells.length; i++) {
                if (ArrayUtils.equals(column.familyName,
                        cells[i].getFamilyArray(),
                        cells[i].getFamilyOffset(),
                        cells[i].getFamilyLength())) {
                    familyCells.add(cells[i]);
                }
            }
            PgDatum.writeJsonbObject(buf, new PairStore(familyCells.toArray(new Cell[familyCells.size()])));
        }
    }

    @Override
    public void close()  {
        try { if (scanner != null) scanner.close(); } catch (Throwable t) {}
        try { if (table != null) table.close(); } catch (Throwable t) {}
    }
}

