package org.bifrost;

import org.bifrost.utils.PairStore;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class PgDatum {
    static final int JB_CMASK = 0x0FFFFFFF;
    static final int JB_FSCALAR	= 0x10000000;
    static final int JB_FOBJECT	= 0x20000000;
    static final int JB_FARRAY = 0x40000000;

    static final int JENTRY_ISSTRING = 0x00000000;
    static final int JENTRY_ISNUMERIC =	0x10000000;;
    static final int JENTRY_ISBOOL_FALSE = 0x20000000;
    static final int JENTRY_ISBOOL_TRUE	= 0x30000000;
    static final int JENTRY_ISNULL = 0x40000000;
    static final int JENTRY_ISCONTAINER = 0x50000000;

    public static int writeTextDatum(ByteBuffer output, String text) throws UnsupportedEncodingException {
        int len = text.length() + 4;
        output.order(ByteOrder.nativeOrder());
        output.putInt(len << 2);
        output.put(text.getBytes("UTF-8"));
        return len;
    }

    public static int writeByteArrayDatum(ByteBuffer output, byte[] text, int offset, int length) throws UnsupportedEncodingException {
        output.order(ByteOrder.nativeOrder());
        int startPos = output.position();
        output.putInt((length + 4) << 2);
        output.put(text, offset, length);
        return output.position() - startPos;
    }

    public static int writeByteArrayDatum(ByteBuffer output, byte[] text) throws UnsupportedEncodingException {
        return writeByteArrayDatum(output, text, 0, text.length);
    }

    public static int writeJsonbObject(ByteBuffer output, PairStore store) throws UnsupportedEncodingException {
        output.order(ByteOrder.nativeOrder());
        int startPos = output.position();
        output.putInt(0);

        // Write tuple header
        int numTuples = store.getLength();
        output.putInt(numTuples | JB_FOBJECT);

        int tupleHeaderPos = output.position();
        // Key/Value headers:
        for (int i = 0; i < store.getLength(); i++)
            output.putInt(store.getKeyLength(i));

        for (int i = 0; i < store.getLength(); i++)
            output.putInt(store.getValueLength(i));

        for (int i = 0; i < store.getLength(); i++)
            output.put(store.getKeyArray(i), store.getKeyOffset(i), store.getKeyLength(i));

        for (int i = 0; i < store.getLength(); i++)
            output.put(store.getValueArray(i), store.getValueOffset(i), store.getValueLength(i));

        int endPos = output.position();
        int len = endPos - startPos;
        output.position(startPos);
        output.putInt(len << 2);
        output.position(endPos);
        return len;
    }

    public static int align(ByteBuffer out, int n) {
        int startPos = out.position();
        while (out.position() % n != 0) {
            out.put((byte)0);
        }
        return out.position() - startPos;
    }
}
