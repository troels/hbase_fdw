package org.bifrost.utils;

import org.apache.hadoop.hbase.Cell;

public class PairStore {
    private Cell[] cells;

    public PairStore(final Cell[] cells) {
        this.cells = cells;
    }

    public byte[] getKeyArray(int i) {
        return cells[i].getQualifierArray();
    }

    public int getKeyOffset(int i) {
        return cells[i].getQualifierOffset();
    }

    public int getKeyLength(int i) {
        return cells[i].getQualifierLength();
    }

    public byte[] getValueArray(int i) {
        return cells[i].getValueArray();
    }

    public int getValueOffset(int i) {
        return cells[i].getValueOffset();
    }

    public int getValueLength(int i) {
        return cells[i].getValueLength();
    }

    public int getLength() {
        return cells.length;
    }

}
