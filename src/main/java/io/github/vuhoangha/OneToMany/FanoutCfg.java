package io.github.vuhoangha.OneToMany;

import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;

@Getter
@Setter
@Accessors(chain = true)
public class FanoutCfg {

    // đường dẫn tới folder chứa dữ liệu
    private String queuePath;

    // port của socket publish msg sang sink
    private Integer realtimePort;

    // port của socket để sink call sang src
    private Integer confirmPort;

    // số lượng msg tối đa gửi "src" --> "sink" trong 1 lần lấy các msg miss
    private Integer numberMsgInBatch;

    // kiểu WaitStrategy được sử dụng lắng nghe các message được ghi vào queue
    private OmniWaitStrategy queueWaitStrategy;

    /*
     * số lượng msg tối đa trong bộ đệm của ZMQ publisher.
     * Khi pub gửi 1 msg nó sẽ đi vào bộ đệm trước,
     * sau đó ZeroMQ sẽ gửi nó sang subscriber qua network
     */
    private Integer maxNumberMsgInCachePub;


    /*
     * thời gian định kỳ đóng file cũ, tạo file mới trong queue
     * mặc định đang để LargeRollCycles.LARGE_DAILY
     *      trong chronicle queue sẽ dùng 1 số long 8 byte = 64 bit để đánh index
     *      1 index sẽ gồm 2 phần là [cycle number][sequence in cycle number]
     *      với LARGE_DAILY, "cycle number" sẽ gồm 27 bit, trừ 1 bit chứa dấu âm dương --> tối đa 2^26=67,108,864 chu kì --> 183,859 năm
     *          "sequence in cycle number" sẽ gồm 37 bit --> "137,438,953,471" item 1 cycle --> 1,590,728 item / giây
     */
    private LargeRollCycles rollCycles;


    // cho phép toàn bộ Fanout chạy trên 1 CPU core riêng ?
    private Boolean enableBindingCore;

    /*
     * cho phép toàn bộ Fanout chạy trên 1 logical processor riêng ?
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer cpu;

    // cho phép Lmax Disruptor chạy trên 1 CPU core riêng ?
    private Boolean enableDisruptorBindingCore;

    /*
     * disruptor chạy trên 1 logical processor riêng
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer disruptorCpu;

    // cho phép Chronicle Queue chạy trên 1 CPU core riêng ?
    private Boolean enableQueueBindingCore;

    /*
     * Chronicle Queue chạy trên 1 logical processor riêng
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer queueCpu;

    // cho phép handler confirm chạy trên 1 CPU core riêng ?
    private Boolean enableHandleConfirmBindingCore;

    /*
     * Handler confirm chạy trên 1 logical processor riêng
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer handleConfirmCpu;

    // có nén data khi ghi ko
    private Boolean compress;


    private FanoutCfg() {
    }

    public static FanoutCfg defaultCfg() {
        FanoutCfg cfg = new FanoutCfg();

        // assign default value
        cfg.setRealtimePort(5555);
        cfg.setConfirmPort(5556);
        cfg.setNumberMsgInBatch(20000);
        cfg.setMaxNumberMsgInCachePub(1000000);
        cfg.setRollCycles(LargeRollCycles.LARGE_DAILY);
        cfg.setQueueWaitStrategy(OmniWaitStrategy.YIELD);
        cfg.setEnableBindingCore(false);
        cfg.setCpu(Constance.CPU_TYPE.ANY);
        cfg.setEnableQueueBindingCore(false);
        cfg.setQueueCpu(Constance.CPU_TYPE.NONE);
        cfg.setEnableHandleConfirmBindingCore(false);
        cfg.setHandleConfirmCpu(Constance.CPU_TYPE.NONE);
        cfg.setCompress(true);

        return cfg;
    }
}
