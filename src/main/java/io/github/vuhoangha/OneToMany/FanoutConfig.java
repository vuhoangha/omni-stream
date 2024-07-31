package io.github.vuhoangha.OneToMany;

import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;

@Getter
@Setter
@Accessors(chain = true)
@NoArgsConstructor
public class FanoutConfig {

    // đường dẫn tới folder chứa dữ liệu
    private String queuePath;

    // port của socket publish msg sang sink
    private Integer realtimePort = 5555;

    // port của socket để Sinkin fetch message từ Fanout
    private Integer fetchingPort = 5556;

    // số lượng msg tối đa gửi "src" --> "sink" trong 1 lần lấy các msg from --> to
    private Integer batchSize = 20_000;

    // kiểu WaitStrategy được sử dụng lắng nghe các message được ghi vào queue
    private OmniWaitStrategy queueWaitStrategy;

    /*
     * thời gian định kỳ đóng file cũ, tạo file mới trong queue
     * mặc định đang để LargeRollCycles.LARGE_DAILY
     *      trong chronicle queue sẽ dùng 1 số long 8 byte = 64 bit để đánh index
     *      1 index sẽ gồm 2 phần là [cycle number][sequence in cycle number]
     *      với LARGE_DAILY, "cycle number" sẽ gồm 27 bit, trừ 1 bit chứa dấu âm dương --> tối đa 2^26=67,108,864 chu kì --> 183,859 năm
     *          "sequence in cycle number" sẽ gồm 37 bit --> "137,438,953,471" item 1 cycle --> 1,590,728 item / giây
     */
    private LargeRollCycles rollCycles = LargeRollCycles.LARGE_DAILY;

    // cấu hình core/cpu cho luồng chính
    private Boolean core;
    private Integer cpu;

    // cấu hình core/cpu cho luồng lắng nghe queue
    private Boolean queueListeningCore;
    private Integer queueListeningCpu;

    // cấu hình core/cpu cho luồng lắng nghe request lấy latest message hoặc from-->to message
    private Boolean messagesFetchingCore;
    private Integer messagesFetchingCpu;

    // có nén data khi ghi ko
    private Boolean compress = true;


    // cân bằng giữa hiệu suất và tiêu tốn CPU
    public static FanoutConfig standardConfig() {
        return new FanoutConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.ANY)
                .setQueueListeningCore(false)
                .setQueueListeningCpu(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.YIELD)
                .setMessagesFetchingCore(false)
                .setMessagesFetchingCpu(Constance.CPU_TYPE.NONE);
    }

    // cho hiệu suất tốt nhất
    public static FanoutConfig bestPerformanceConfig() {
        return new FanoutConfig()
                .setCore(true)
                .setCpu(Constance.CPU_TYPE.NONE)
                .setQueueListeningCore(true)
                .setQueueListeningCpu(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.BUSY)
                .setMessagesFetchingCore(true)
                .setMessagesFetchingCpu(Constance.CPU_TYPE.NONE);
    }

    // tiết kiệm CPU nhất
    public static FanoutConfig minimalCpuConfig() {
        return new FanoutConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.NONE)
                .setQueueListeningCore(false)
                .setQueueListeningCpu(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.SLEEP)
                .setMessagesFetchingCore(false)
                .setMessagesFetchingCpu(Constance.CPU_TYPE.NONE);
    }
}
