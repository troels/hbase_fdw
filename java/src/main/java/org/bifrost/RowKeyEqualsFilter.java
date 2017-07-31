package org.bifrost;

import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyEqualsFilter implements HBaseFilter {
    private final byte[] rowKey;
    private boolean canReturnAnything = true;

    public RowKeyEqualsFilter(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    @Override
    public boolean apply(Scan scan) {
        if (scan.getStartRow() != null && Bytes.compareTo(rowKey, scan.getStartRow()) < 0) {
            return false;
        }
        scan.setStartRow(rowKey);

        byte[] stopRow = new byte[rowKey.length + 1];
        System.arraycopy(rowKey, 0, stopRow, 0, rowKey.length);
        stopRow[rowKey.length] = 0;

        if (scan.getStopRow() != null && scan.getStopRow().length > 0 && Bytes.compareTo(stopRow, scan.getStopRow()) > 0) {
            return false;
        }
        scan.setStopRow(stopRow);
        return true;
    }
}
