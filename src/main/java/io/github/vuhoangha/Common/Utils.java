package io.github.vuhoangha.Common;

import io.github.vuhoangha.OneToMany.Fanout;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.LockSupport;

@Slf4j
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
     * @param name                      tên của logic code này
     * @param isMainFlow                đây có phải là luồng chính hay ko
     * @param processBindingCore        luồng chính có gắn vào 1 CPU core ko
     * @param processCpu                luồng chính có gắn vào logical processor nào ko
     * @param enableSpecificBindingCore luồng phụ có gắn vào 1 CPU core ko
     * @param cpu                       luồng phụ có gắn vào logical processor nào ko
     * @param coreFunc                  logic code cần chạy
     * @return chứa lock và thread chạy luồng này
     */
    public static AffinityCompose runWithThreadAffinity(
            String name,
            Boolean isMainFlow,
            Boolean processBindingCore,
            Integer processCpu,
            Boolean enableSpecificBindingCore,
            Integer cpu,
            Runnable coreFunc) {
        try {
            CompletableFuture<AffinityLock> cb = new CompletableFuture<>();

            Thread thread = new Thread(() -> {
                if (!isMainFlow && (processBindingCore || processCpu >= Constance.CPU_TYPE.ANY)) {
                    // cả Fanout chạy chung 1 CPU core hoặc 1 logical processor
                    coreFunc.run();
                    cb.complete(null);
                } else if (enableSpecificBindingCore) {
                    // chạy trên 1 CPU core riêng
                    AffinityLock al = AffinityLock.acquireCore();
                    coreFunc.run();
                    cb.complete(al);
                } else if (cpu == Constance.CPU_TYPE.NONE) {
                    // chạy như 1 thread bình thường, do hệ điều hành quản lý và phân phối tới các logical processor
                    coreFunc.run();
                    cb.complete(null);
                } else if (cpu == Constance.CPU_TYPE.ANY) {
                    // chạy trên 1 logical processor ngẫu nhiên
                    AffinityLock al = AffinityLock.acquireLock();
                    coreFunc.run();
                    cb.complete(al);
                } else if (cpu > Constance.CPU_TYPE.ANY) {
                    // chạy trên 1 logical processor chỉ định
                    AffinityLock al = AffinityLock.acquireLock(cpu);
                    coreFunc.run();
                    cb.complete(al);
                } else {
                    // cấu hình lỗi rồi
                    LOGGER.error(MessageFormat.format("Config {0} invalid. Stop now !", name));
                    cb.complete(null);
                }

                // giữ thread sống để việc lock vào CPU core / logical processor ko bị tranh chấp nhau
                // khi process close sẽ giải phóng thread này
                LockSupport.park();
            });
            thread.start();

            AffinityCompose affinityCompose = new AffinityCompose();
            affinityCompose.thread = thread;
            affinityCompose.lock = cb.get();
            return affinityCompose;
        } catch (Exception ex) {
            LOGGER.error(MessageFormat.format("Config {0} exception", name), ex);
            return new AffinityCompose();
        }
    }


    public static void checkNull(Object obj, String errorMessage) {
        if (obj == null) {
            LOGGER.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
    }


    public static void benchmark(String name, Runnable task) {
        long start = System.currentTimeMillis();
        task.run();
        long total = System.currentTimeMillis() - start;
        log.info("{} exec time: {} ms", name, total);
    }


    public static void deleteFolder(String path) {
        File folder = new File(path);
        if (folder.exists() && folder.isDirectory()) {
            deleteFolder(folder);
        }
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteFolder(file);
                } else {
                    file.delete();
                }
            }
        }
        folder.delete();
    }

    public static void deleteOldFiles(String directoryPath, long secondsOld, String endsOfFile) {
        try {
            Path directory = Paths.get(directoryPath);

            // các file chỉnh sửa trước thời gian này sẽ bị xóa
            Instant cutoffTime = Instant.now().minus(secondsOld, ChronoUnit.SECONDS);

            // Duyệt qua các file trong thư mục
            Files.list(directory)
                    .filter(Files::isRegularFile) // Chỉ xét các file thông thường
                    .filter(path -> path.toString().endsWith(endsOfFile)) // Chỉ xét các file có đuôi mong muốn
                    .filter(path -> {
                        try {
                            // Kiểm tra thời gian sửa đổi cuối cùng của file
                            Instant lastModifiedTime = Files.getLastModifiedTime(path).toInstant();
                            return lastModifiedTime.isBefore(cutoffTime); // Xóa nếu cũ hơn cutoffTime
                        } catch (IOException e) {
                            log.error("Error getting last modified time for {}", path, e);
                            return false; // Không xóa nếu có lỗi
                        }
                    })
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                            log.info("Deleted: {}", path);
                        } catch (IOException e) {
                            log.info("Error deleting file {}", path, e);
                        }
                    });
        } catch (Exception e) {
            log.error("deleteOldFiles in folder {} error", directoryPath, e);
        }
    }


}
