package org.bifrost;

import org.apache.hadoop.hbase.client.Scan;

import java.util.ArrayList;
import java.util.List;

public class HBaseFilterCreator {
    public List<HBaseFilter> filters = new ArrayList<>();
    public HBaseFilterCreator() {}

    public void addRowKeyEqualsFilter(byte[] rowKey) {
        filters.add(new RowKeyEqualsFilter(rowKey));
    }

    public boolean applyFilters(Scan scan) {
        for (HBaseFilter filter: filters) {
            if (!filter.apply(scan)) {
                return false;
            }
        }
        return true;
    }
}
