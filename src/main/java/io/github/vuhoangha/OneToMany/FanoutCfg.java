package io.github.vuhoangha.OneToMany;

import com.lmax.disruptor.WaitStrategy;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;

public class FanoutCfg {

    // đường dẫn tới folder chứa dữ liệu
    private String queuePath;

    // port của socket publish msg sang sink
    private Integer realtimePort;

    // port của socket để sink call sang src
    private Integer confirmPort;

    // số lượng msg tối đa gửi "src" --> "sink" trong 1 lần lấy các msg miss
    private Integer numberMsgInBatch;

    // kiểu WaitStrategy được sử dụng để gom msg từ nhiều thread ghi vào queue
    private WaitStrategy waitStrategy;

    // kích cỡ ring buffer của disruptor gửi/nhận msg. Phải là dạng 2^n
    private Integer ringBufferSize;

    /*
     * số lượng msg tối đa trong bộ đệm của ZMQ publisher.
     * Khi pub gửi 1 msg nó sẽ đi vào bộ đệm trước,
     * sau đó ZeroMQ sẽ gửi nó sang subscriber qua network
     */
    private Integer maxNumberMsgInCachePub;

    /*
     * version (-128 --> 127)
     * phiên bản của tin nhắn
     */
    private Byte version;

    /*
     * thời gian định kỳ đóng file cũ, tạo file mới trong queue
     * mặc định đang để LargeRollCycles.LARGE_DAILY
     *      trong chronicle queue sẽ dùng 1 số long 8 byte = 64 bit để đánh index
     *      1 index sẽ gồm 2 phần là [cycle number][sequence in cycle number]
     *      với LARGE_DAILY, "cycle number" sẽ gồm 27 bit, trừ 1 bit chứa dấu âm dương --> tối đa 2^26=67,108,864 chu kì --> 183,859 năm
     *          "sequence in cycle number" sẽ gồm 37 bit --> "137,438,953,471" item 1 cycle --> 1,590,728 item / giây
     */
    private LargeRollCycles rollCycles;


    public FanoutCfg() {
    }

    public static FanoutCfg builder() {
        return new FanoutCfg();
    }


    public LargeRollCycles getRollCycles() {
        return rollCycles;
    }

    public FanoutCfg setRollCycles(LargeRollCycles rollCycles) {
        this.rollCycles = rollCycles;
        return this;
    }

    public Integer getRingBufferSize() {
        return ringBufferSize;
    }

    public FanoutCfg setRingBufferSize(Integer ringBufferSize) {
        this.ringBufferSize = ringBufferSize;
        return this;
    }

    public WaitStrategy getWaitStrategy() {
        return waitStrategy;
    }

    public FanoutCfg setWaitStrategy(WaitStrategy waitStrategy) {
        this.waitStrategy = waitStrategy;
        return this;
    }

    public String getQueuePath() {
        return queuePath;
    }

    public FanoutCfg setQueuePath(String queuePath) {
        this.queuePath = queuePath;
        return this;
    }

    public Integer getRealtimePort() {
        return realtimePort;
    }

    public FanoutCfg setRealtimePort(Integer realtimePort) {
        this.realtimePort = realtimePort;
        return this;
    }

    public Integer getConfirmPort() {
        return confirmPort;
    }

    public FanoutCfg setConfirmPort(Integer confirmPort) {
        this.confirmPort = confirmPort;
        return this;
    }

    public Integer getNumberMsgInBatch() {
        return numberMsgInBatch;
    }

    public FanoutCfg setNumberMsgInBatch(Integer numberMsgInBatch) {
        this.numberMsgInBatch = numberMsgInBatch;
        return this;
    }

    public Integer getMaxNumberMsgInCachePub() {
        return maxNumberMsgInCachePub;
    }

    public FanoutCfg setMaxNumberMsgInCachePub(Integer maxNumberMsgInCachePub) {
        this.maxNumberMsgInCachePub = maxNumberMsgInCachePub;
        return this;
    }

    public Byte getVersion() {
        return version;
    }

    public FanoutCfg setVersion(Byte version) {
        this.version = version;
        return this;
    }
}
