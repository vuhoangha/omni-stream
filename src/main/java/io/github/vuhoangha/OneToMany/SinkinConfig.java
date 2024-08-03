package io.github.vuhoangha.OneToMany;

import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;

import java.text.MessageFormat;

@Getter
@Setter
@Accessors(chain = true)
public class SinkinConfig {

    // folder chứa data của queue
    private String queuePath;

    // IP của Fanout host
    private String sourceIP;

    // port của Fanout dùng để publish dữ liệu cho các Sinkin
    private Integer realtimePort = 5555;

    // port của Fanout dùng để response các yêu cầu lấy message bị miss của Sinkin
    private Integer dealerPort = 5556;

    // 1 msg ở trong hàng chờ quá lâu đồng nghĩa với việc các msg trước nó đã bị loss, Sinkin sẽ request sang Fanout để lấy các message đó
    private Integer messageExpirationDuration = 2_000;

    // số lượng msg tối đa trong ObjectsPool dùng để chứa lại các message chờ xử lý
    private Integer messagePoolSize = 30_000;

    // kích cỡ bộ đệm ZeroMQ SUB chứa các message đã nhận được nhưng chưa xử lý (số lượng message)
    private Integer zmqSubBufferSize = 10_000_000;

    // thời gian định kỳ lấy message mới nhất từ Fanout (miliseconds)
    private Integer latestMessageFetchInterval = 3_000;

    // thời gian định kỳ check và lấy các message bị thiếu
    private Integer lostMessageScanInterval = 3_000;

    // dữ liệu nhận được có phải đã được nén ko
    private Boolean compress = true;

    // bắt đầu đọc queue từ index nào. Nếu bằng "-1" thì nó sẽ đọc từ đầu
    private Long startId = -2L;

    // tên của người đọc. Nó sẽ dùng làm ID để sau khi restart, ta sẽ tiếp tục đọc từ vị trí cũ trong queue chứ ko phải đọc từ đầu
    private String readerName;

    /*
     * thời gian định kỳ đóng file cũ, tạo file mới trong queue
     * mặc định đang để LargeRollCycles.LARGE_DAILY
     *      trong chronicle queue sẽ dùng 1 số long 8 byte = 64 bit để đánh index
     *      1 index sẽ gồm 2 phần là [cycle number][sequence in cycle number]
     *      với LARGE_DAILY, "cycle number" sẽ gồm 27 bit, trừ 1 bit chứa dấu âm dương --> tối đa 2^26=67,108,864 chu kì --> 183,859 năm
     *          "sequence in cycle number" sẽ gồm 37 bit --> "137,438,953,471" item 1 cycle --> 1,590,728 item / giây
     * tham khảo: https://github.com/OpenHFT/Chronicle-Queue/blob/ea/docs/FAQ.adoc#how-to-change-the-time-that-chronicle-queue-rolls
     */
    private LargeRollCycles rollCycles = LargeRollCycles.LARGE_DAILY;

    // chạy trên 1 CPU core hoặc logical processor (Constance.CPU_TYPE) cụ thể ?
    private Boolean processorCore;
    private Integer processorCpu;

    // chạy trên 1 CPU core hoặc logical processor (Constance.CPU_TYPE) cụ thể ?
    private Boolean queueCore;
    private Integer queueCpu;

    // kiểu WaitStrategy được sử dụng lắng nghe các item mới được ghi vào queue
    private OmniWaitStrategy queueWaitStrategy;


    public String getRealTimeUrl() {
        return MessageFormat.format("tcp://{0}:{1}", sourceIP, realtimePort + "");
    }

    public String getDealerUrl() {
        return MessageFormat.format("tcp://{0}:{1}", sourceIP, dealerPort + "");
    }

    public static SinkinConfig standardConfig() {
        return new SinkinConfig()
                .setProcessorCore(false)
                .setProcessorCpu(Constance.CPU_TYPE.ANY)
                .setQueueCore(false)
                .setQueueCpu(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.SLEEP);
    }

    public static SinkinConfig bestPerformanceConfig() {
        return new SinkinConfig()
                .setProcessorCore(true)
                .setProcessorCpu(Constance.CPU_TYPE.NONE)
                .setQueueCore(true)
                .setQueueCpu(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.BUSY);
    }

    public static SinkinConfig minimalCpuConfig() {
        return new SinkinConfig()
                .setProcessorCore(false)
                .setProcessorCpu(Constance.CPU_TYPE.NONE)
                .setQueueCore(false)
                .setQueueCpu(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.SLEEP);
    }

}
