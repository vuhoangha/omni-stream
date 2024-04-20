package io.github.vuhoangha.Common;

import io.github.vuhoangha.OneToMany.Fanout;
import net.openhft.affinity.AffinityLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

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


    /**
     * Chạy 1 function trên CPU core / Logical processor
     *
     * @param name tên của logic code này
     * @param isMainFlow đây có phải là luồng chính hay ko
     * @param processBindingCore luồng chính có gắn vào 1 CPU core ko
     * @param processCpu luồng chính có gắn vào logical processor nào ko
     * @param enableSpecificBindingCore luồng phụ có gắn vào 1 CPU core ko
     * @param cpu luồng phụ có gắn vào logical processor nào ko
     * @param coreFunc logic code cần chạy
     */
    public static void runWithThreadAffinity(
            String name,
            Boolean isMainFlow,
            Boolean processBindingCore,
            Integer processCpu,
            Boolean enableSpecificBindingCore,
            Integer cpu,
            Runnable coreFunc) {
        if (!isMainFlow && (processBindingCore || processCpu >= -1)) {
            // cả Fanout chạy chung 1 CPU core hoặc 1 logical processor
            coreFunc.run();
        } else if (enableSpecificBindingCore) {
            // chạy trên 1 CPU core riêng
            try (AffinityLock al = AffinityLock.acquireCore()) {
                coreFunc.run();
            }
        } else if (cpu == -2) {
            // chạy như 1 thread bình thường, do hệ điều hành quản lý và phân phối tới các logical processor
            coreFunc.run();
        } else if (cpu == -1) {
            // chạy trên 1 logical processor ngẫu nhiên
            try (AffinityLock al = AffinityLock.acquireLock()) {
                coreFunc.run();
            }
        } else if (cpu >= 0) {
            // chạy trên 1 logical processor chỉ định
            try (AffinityLock al = AffinityLock.acquireLock(cpu)) {
                coreFunc.run();
            }
        } else {
            // cấu hình lỗi rồi
            LOGGER.error(MessageFormat.format("Config {0} invalid. Stop now !", name));
        }
    }

}
