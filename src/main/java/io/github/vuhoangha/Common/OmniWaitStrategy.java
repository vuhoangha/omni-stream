package io.github.vuhoangha.Common;

import java.util.concurrent.locks.LockSupport;

public enum OmniWaitStrategy {

    /*
     * sử dụng LockSupport.parkNanos(1): cho CPU nghỉ ngơi 1 nanoseconds
     *      Thời gian nghỉ thực tế phụ thuộc vào hệ điều hành
     *      Linux thông thường là 60 microseconds
     *      Ngoài thời gian sleep, còn chú ý đến thời gian để hệ điều hành đánh thức Thread này dậy bằng bộ lập lịch. Vậy nên sẽ có thêm độ trễ
     *      Tham khảo: https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/
     */
    SLEEP(1),

    // Thread.yield(): nhường CPU cho thread khác thực thi. Nếu ko có thread nào thì lại chạy tiếp Thread.yield()
    YIELD(2),

    // chạy 1 vòng lặp liên tục không nghỉ ngơi. Là chiến lược có latency thấp nhất, performance cao nhất nhưng tốn kém CPU nhất
    BUSY(3);


    private final int value;

    OmniWaitStrategy(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static Runnable getWaiter(OmniWaitStrategy strategy) {
        if (strategy == SLEEP) {
            return () -> LockSupport.parkNanos(1);
        } else if (strategy == YIELD) {
            return Thread::yield;
        } else {
            return () -> {
            };
        }
    }

}
