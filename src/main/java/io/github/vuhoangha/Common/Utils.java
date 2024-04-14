package io.github.vuhoangha.Common;

public class Utils {

    public static byte[] longToBytes(long l) {
        try {
            return new byte[]{
                    (byte) (l >> 56),
                    (byte) (l >> 48),
                    (byte) (l >> 40),
                    (byte) (l >> 32),
                    (byte) (l >> 24),
                    (byte) (l >> 16),
                    (byte) (l >> 8),
                    (byte) (l)
            };
        } catch (Exception ex) {
            return null;
        }
    }


    public static long bytesToLong(byte[] bytes) {
        try {
            return ((long) bytes[0] << 56) |
                    ((long) bytes[1] & 0xFF) << 48 |
                    ((long) bytes[2] & 0xFF) << 40 |
                    ((long) bytes[3] & 0xFF) << 32 |
                    ((long) bytes[4] & 0xFF) << 24 |
                    ((long) bytes[5] & 0xFF) << 16 |
                    ((long) bytes[6] & 0xFF) << 8 |
                    ((long) bytes[7] & 0xFF);
        } catch (Exception ex) {
            return -1;
        }
    }

    public static long bytesChronicleToLong(byte[] bytes) {
        try {
            return ((long) bytes[7] << 56) |
                    ((long) bytes[6] & 0xFF) << 48 |
                    ((long) bytes[5] & 0xFF) << 40 |
                    ((long) bytes[4] & 0xFF) << 32 |
                    ((long) bytes[3] & 0xFF) << 24 |
                    ((long) bytes[2] & 0xFF) << 16 |
                    ((long) bytes[1] & 0xFF) << 8 |
                    ((long) bytes[0] & 0xFF);
        } catch (Exception ex) {
            return -1;
        }
    }


    public static byte booleanToByte(boolean a) {
        return (byte) (a ? 1 : 0);
    }


    public static boolean byteToBoolean(byte a) {
        return a != 0;
    }

}
