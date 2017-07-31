package org.bifrost.utils;


public class ArrayUtils {
    public static boolean equals(byte[] a, int aoffset, int alength, byte[] b, int boffset, int blength) {
        if (alength != blength)
            return false;

        for (int i = 0; i < alength; i++) {
            if (a[i + aoffset] != b[i + boffset]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(byte[] a, byte[] b, int boffset, int blength) {
        return equals(a, 0, a.length, b, boffset, blength);
    }
}
