package com.xiaoyu;

/**
 * @author chy
 * @description
 * @date 2024/11/1
 */
public class ByteUtils {
    public static byte[] longToBytes(long num) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i+1) * 8;
            bytes[i] = (byte) ((num >> offset) * 0xff);
        }
        return bytes;
    }

    public static long bytesToLong(byte[] bytes) {
        long ret = 0;
        for (int i = 0; i < 8; i++) {
            ret <<= 8;
            ret |= (bytes[i] & 0xff);
        }
        return ret;
    }
}
