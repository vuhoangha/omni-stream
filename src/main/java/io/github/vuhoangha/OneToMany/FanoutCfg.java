package io.github.vuhoangha.OneToMany;

import com.lmax.disruptor.WaitStrategy;
import io.github.vuhoangha.Common.OmniWaitStrategy;
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
    private WaitStrategy disruptorWaitStrategy;

    // kích cỡ ring buffer của disruptor gửi/nhận msg. Phải là dạng 2^n
    private Integer ringBufferSize;

    // kiểu WaitStrategy được sử dụng lắng nghe các message được ghi vào queue
    private OmniWaitStrategy queueWaitStrategy;

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


    public FanoutCfg() {
    }

    public static FanoutCfg builder() {
        return new FanoutCfg();
    }


    public Boolean getEnableHandleConfirmBindingCore() {
        return enableHandleConfirmBindingCore;
    }

    public FanoutCfg setEnableHandleConfirmBindingCore(Boolean enableHandleConfirmBindingCore) {
        this.enableHandleConfirmBindingCore = enableHandleConfirmBindingCore;
        return this;
    }

    public Integer getHandleConfirmCpu() {
        return handleConfirmCpu;
    }

    public FanoutCfg setHandleConfirmCpu(Integer handleConfirmCpu) {
        this.queueCpu = handleConfirmCpu;
        return this;
    }

    public Boolean getEnableQueueBindingCore() {
        return enableQueueBindingCore;
    }

    public FanoutCfg setEnableQueueBindingCore(Boolean enableQueueBindingCore) {
        this.enableQueueBindingCore = enableQueueBindingCore;
        return this;
    }

    public Integer getQueueCpu() {
        return queueCpu;
    }

    public FanoutCfg setQueueCpu(Integer queueCpu) {
        this.queueCpu = queueCpu;
        return this;
    }

    public Integer getCpu() {
        return cpu;
    }

    public FanoutCfg setCpu(Integer cpu) {
        this.cpu = cpu;
        return this;
    }

    public Boolean getEnableBindingCore() {
        return enableBindingCore;
    }

    public FanoutCfg setEnableBindingCore(Boolean enableBindingCore) {
        this.enableBindingCore = enableBindingCore;
        return this;
    }

    public Boolean getEnableDisruptorBindingCore() {
        return enableDisruptorBindingCore;
    }

    public FanoutCfg setEnableDisruptorBindingCore(Boolean enableDisruptorBindingCore) {
        this.enableDisruptorBindingCore = enableDisruptorBindingCore;
        return this;
    }

    public Integer getDisruptorCpu() {
        return disruptorCpu;
    }

    public FanoutCfg setDisruptorCpu(Integer disruptorCpu) {
        this.disruptorCpu = disruptorCpu;
        return this;
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

    public WaitStrategy getDisruptorWaitStrategy() {
        return disruptorWaitStrategy;
    }

    public FanoutCfg setDisruptorWaitStrategy(WaitStrategy disruptorWaitStrategy) {
        this.disruptorWaitStrategy = disruptorWaitStrategy;
        return this;
    }

    public OmniWaitStrategy getQueueWaitStrategy() {
        return queueWaitStrategy;
    }

    public FanoutCfg setQueueWaitStrategy(OmniWaitStrategy queueWaitStrategy) {
        this.queueWaitStrategy = queueWaitStrategy;
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
