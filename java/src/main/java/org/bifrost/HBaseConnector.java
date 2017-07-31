package org.bifrost;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableSet;

public class HBaseConnector {
    private final Configuration conf;
    private Connection conn;

    public HBaseConnector() {
        conf = HBaseConfiguration.create();
    }

    private boolean fetchData(final Scan scan,  final HBaseFilterCreator creator, final PgHbaseColumn[] columns) {
        for (PgHbaseColumn column: columns) {
            if (column.row) continue;
            if (column.family) {
                scan.addFamily(column.familyName);
            }
            if (column.qualifier) {
                Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
                if (!familyMap.containsKey(column.familyName) ||
                    familyMap.get(column.familyName) != null)
                    scan.addColumn(column.familyName, column.qualifierName);
            }
        }

        if (!creator.applyFilters(scan)) {
            return false;
        }
        return true;
    }

    public Scanner makeScanner(final byte[] tableName, final PgHbaseColumn[] columns, final HBaseFilterCreator filterCreator) throws IOException {
        final Scan scan = new Scan();
        boolean anyResults = fetchData(scan, filterCreator, columns);
        if (!anyResults) {
            return new HBaseToPgScanner(null, null, columns);
        }

        connect();

        final Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            final ResultScanner scanner = table.getScanner(scan);
            return new HBaseToPgScanner(table, scanner, columns);
        } catch (Throwable t) {
            table.close();
            throw t;
        }

    }

    private void connect() throws IOException {
        if (conn != null) return;
        synchronized(this) {
            if (conn != null) return;
            conn = ConnectionFactory.createConnection(conf);
        }
    }
}

