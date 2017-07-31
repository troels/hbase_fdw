package org.bifrost;

public class PgHbaseColumn {
    public final boolean qualifier;
    public final boolean family;
    public final boolean row;

    public final byte[] familyName;
    public final byte[] qualifierName;

    public PgHbaseColumn(boolean row, boolean family, boolean qualifier,
                         byte[] familyName, byte[] qualifierName)
    {
        this.row = row;
        this.family = family;
        this.qualifier = qualifier;
        this.familyName = familyName;
        this.qualifierName = qualifierName;
    }
}
