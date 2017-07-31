package org.bifrost;


import org.apache.hadoop.hbase.client.Scan;

public interface HBaseFilter {
    public boolean apply(Scan scan);

}
