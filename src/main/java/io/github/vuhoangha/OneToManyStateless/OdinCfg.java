package io.github.vuhoangha.OneToManyStateless;

import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import io.github.vuhoangha.Common.Constance;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Arrays;
import java.util.List;

@Getter
@Setter
@Accessors(chain = true)
public class OdinCfg {

    // port của socket publish msg sang sink
    private List<Integer> realtimePorts;

    // port của socket để sink call sang src
    private Integer confirmPort;

    // kiểu WaitStrategy được sử dụng để gom msg từ nhiều thread ghi vào queue
    private WaitStrategy disruptorWaitStrategy;

    // kích cỡ ring buffer của disruptor gửi/nhận msg. Phải là dạng 2^n
    private Integer ringBufferSize;

    /*
     * số lượng msg tối đa trong bộ đệm của ZMQ publisher.
     * Khi pub gửi 1 msg nó sẽ đi vào bộ đệm trước,
     * sau đó ZeroMQ sẽ gửi nó sang subscriber qua network
     */
    private Integer maxNumberMsgInCachePub;


    // cho phép toàn bộ Odin chạy trên 1 CPU core riêng ?
    private Boolean enableBindingCore;

    /*
     * cho phép toàn bộ Odin chạy trên 1 logical processor riêng ?
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

    /*
     * disruptor handler chạy trên 1 logical processor riêng
     * ý nghĩa các giá trị tương tự như trên
     */
    private Integer disruptorHandlerCpu;

    // cho phép handler confirm chạy trên 1 CPU core riêng ?
    private Boolean enableHandleConfirmBindingCore;

    /*
     * Handler confirm chạy trên 1 logical processor riêng
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer handleConfirmCpu;

    // thời gian msg nằm trong cache (ms)
    private Integer odinCacheExpiryTime;

    // số lượng item "OdinCacheEvent" trong object pool
    private Integer objectPoolSize;


    private OdinCfg() {
    }


    public static OdinCfg getDefault() {
        OdinCfg cfg = new OdinCfg();
        cfg.setRealtimePorts(Arrays.asList(5570));
        cfg.setConfirmPort(5571);
        cfg.setMaxNumberMsgInCachePub(1000000);
        cfg.setDisruptorWaitStrategy(new YieldingWaitStrategy());
        cfg.setRingBufferSize(2 << 16);     // 131072
        cfg.setEnableBindingCore(false);
        cfg.setCpu(Constance.CPU_TYPE.ANY);
        cfg.setEnableDisruptorBindingCore(false);
        cfg.setDisruptorCpu(Constance.CPU_TYPE.NONE);
        cfg.setDisruptorHandlerCpu(Constance.CPU_TYPE.NONE);
        cfg.setEnableHandleConfirmBindingCore(false);
        cfg.setHandleConfirmCpu(Constance.CPU_TYPE.NONE);
        cfg.setOdinCacheExpiryTime(60000);
        cfg.setObjectPoolSize(100000);
        return cfg;
    }

}
