package io.github.vuhoangha.OneToMany;

import com.lmax.disruptor.WaitStrategy;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;

import java.text.MessageFormat;

public class SinkinCfg {

    // folder chứa data của queue
    private String queuePath;

    // IP của Fanout host
    private String sourceIP;

    // port của Fanout dùng để publish dữ liệu cho các Sinkin
    private Integer realtimePort;

    // port của Fanout dùng để response các yêu cầu lấy message bị miss của Sinkin
    private Integer confirmPort;

    // 1 msg ở trong hàng chờ quá lâu thì sẽ bị coi là thiếu các message trước nó, Sinkin sẽ request sang Fanout để lấy các message đó
    private Integer maxTimeWaitMS;

    // số lượng msg tối đa trong ObjectsPool dùng để chứa lại các message chờ xử lý
    private Integer maxObjectsPoolWait;

    // kích cỡ tối đa bộ đệm ZeroMQ SUB chờ được xử lý (số lượng message)
    private Integer zmqSubBufferSize;

    // thời gian định kỳ lấy message mới nhất từ Fanout (miliseconds)
    private Integer timeRateGetLatestMsgMS;

    // thời gian định kỳ check và lấy các message bị thiếu
    private Integer timeRateGetMissMsgMS;

    // timeout send khi lấy message miss của Fanout
    private Integer timeoutSendReqMissMsg;

    // timeout receive khi lấy message miss của Fanout
    private Integer timeoutRecvReqMissMsg;

    // kiểu WaitStrategy trong Lmax Disruptor để gom message lại và xử lý
    // tham khảo: https://lmax-exchange.github.io/disruptor/user-guide/index.html
    private WaitStrategy waitStrategy;

    // kích cỡ ring buffer của disruptor xử lý message. Phải là dạng 2^n
    private Integer ringBufferSize;

    /*
     * thời gian định kỳ đóng file cũ, tạo file mới trong queue
     * mặc định đang để LargeRollCycles.LARGE_DAILY
     *      trong chronicle queue sẽ dùng 1 số long 8 byte = 64 bit để đánh index
     *      1 index sẽ gồm 2 phần là [cycle number][sequence in cycle number]
     *      với LARGE_DAILY, "cycle number" sẽ gồm 27 bit, trừ 1 bit chứa dấu âm dương --> tối đa 2^26=67,108,864 chu kì --> 183,859 năm
     *          "sequence in cycle number" sẽ gồm 37 bit --> "137,438,953,471" item 1 cycle --> 1,590,728 item / giây
     * tham khảo: https://github.com/OpenHFT/Chronicle-Queue/blob/ea/docs/FAQ.adoc#how-to-change-the-time-that-chronicle-queue-rolls
     */
    private LargeRollCycles rollCycles;

    // cho phép toàn bộ Sinkin chạy trên 1 CPU core riêng ?
    private Boolean enableBindingCore;

    /*
     * cho phép toàn bộ Sinkin chạy trên 1 logical processor riêng ?
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer cpu;

    // cho phép Disruptor process msg chạy trên 1 CPU core riêng ?
    private Boolean enableDisruptorProcessMsgBindingCore;

    /*
     * Disruptor process msg chạy trên 1 logical processor riêng
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer disruptorProcessMsgCpu;

    // cho phép việc check các msg bị miss và sub các item mới trong queue chạy trên 1 CPU core riêng ?
    private Boolean enableCheckMissMsgAndSubQueueBindingCore;

    /*
     * Check các msg bị miss và sub các item mới trong queue msg chạy trên 1 logical processor riêng
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer checkMissMsgAndSubQueueCpu;

    // cho phép sub các msg mới chạy trên 1 CPU core riêng ?
    private Boolean enableSubMsgBindingCore;

    /*
     * Sub các msg mới chạy trên 1 logical processor riêng
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer subMsgCpu;

    // kiểu WaitStrategy được sử dụng lắng nghe các item mới được ghi vào queue
    private OmniWaitStrategy queueWaitStrategy;


    public SinkinCfg() {
    }

    public static SinkinCfg builder() {
        return new SinkinCfg();
    }

    public OmniWaitStrategy getQueueWaitStrategy() {
        return queueWaitStrategy;
    }

    public SinkinCfg setQueueWaitStrategy(OmniWaitStrategy queueWaitStrategy) {
        this.queueWaitStrategy = queueWaitStrategy;
        return this;
    }

    public Integer getSubMsgCpu() {
        return subMsgCpu;
    }

    public SinkinCfg setSubMsgCpu(Integer subMsgCpu) {
        this.subMsgCpu = subMsgCpu;
        return this;
    }

    public Boolean getEnableSubMsgBindingCore() {
        return enableSubMsgBindingCore;
    }

    public SinkinCfg setEnableSubMsgBindingCore(Boolean enableSubMsgBindingCore) {
        this.enableSubMsgBindingCore = enableSubMsgBindingCore;
        return this;
    }


    public Integer getCheckMissMsgAndSubQueueCpu() {
        return checkMissMsgAndSubQueueCpu;
    }

    public SinkinCfg setCheckMissMsgAndSubQueueCpu(Integer checkMissMsgAndSubQueueCpu) {
        this.checkMissMsgAndSubQueueCpu = checkMissMsgAndSubQueueCpu;
        return this;
    }

    public Boolean getEnableCheckMissMsgAndSubQueueBindingCore() {
        return enableCheckMissMsgAndSubQueueBindingCore;
    }

    public SinkinCfg setEnableCheckMissMsgAndSubQueueBindingCore(Boolean enableCheckMissMsgAndSubQueueBindingCore) {
        this.enableCheckMissMsgAndSubQueueBindingCore = enableCheckMissMsgAndSubQueueBindingCore;
        return this;
    }

    public Integer getDisruptorProcessMsgCpu() {
        return disruptorProcessMsgCpu;
    }

    public SinkinCfg setDisruptorProcessMsgCpu(Integer disruptorProcessMsgCpu) {
        this.disruptorProcessMsgCpu = disruptorProcessMsgCpu;
        return this;
    }

    public Boolean getEnableDisruptorProcessMsgBindingCore() {
        return enableDisruptorProcessMsgBindingCore;
    }

    public SinkinCfg setEnableDisruptorProcessMsgBindingCore(Boolean enableDisruptorProcessMsgBindingCore) {
        this.enableDisruptorProcessMsgBindingCore = enableDisruptorProcessMsgBindingCore;
        return this;
    }

    public Integer getCpu() {
        return cpu;
    }

    public SinkinCfg setCpu(Integer cpu) {
        this.cpu = cpu;
        return this;
    }

    public Boolean getEnableBindingCore() {
        return enableBindingCore;
    }

    public SinkinCfg setEnableBindingCore(Boolean enableBindingCore) {
        this.enableBindingCore = enableBindingCore;
        return this;
    }

    public LargeRollCycles getRollCycles() {
        return rollCycles;
    }

    public SinkinCfg setRollCycles(LargeRollCycles rollCycles) {
        this.rollCycles = rollCycles;
        return this;
    }

    public Integer getRingBufferSize() {
        return ringBufferSize;
    }

    public SinkinCfg setRingBufferSize(Integer ringBufferSize) {
        this.ringBufferSize = ringBufferSize;
        return this;
    }

    public WaitStrategy getWaitStrategy() {
        return waitStrategy;
    }

    public SinkinCfg setWaitStrategy(WaitStrategy waitStrategy) {
        this.waitStrategy = waitStrategy;
        return this;
    }

    public Integer getTimeoutRecvReqMissMsg() {
        return timeoutRecvReqMissMsg;
    }

    public SinkinCfg setTimeoutRecvReqMissMsg(Integer timeoutRecvReqMissMsg) {
        this.timeoutRecvReqMissMsg = timeoutRecvReqMissMsg;
        return this;
    }

    public Integer getTimeoutSendReqMissMsg() {
        return timeoutSendReqMissMsg;
    }

    public SinkinCfg setTimeoutSendReqMissMsg(Integer timeoutSendReqMissMsg) {
        this.timeoutSendReqMissMsg = timeoutSendReqMissMsg;
        return this;
    }

    public Integer getTimeRateGetMissMsgMS() {
        return timeRateGetMissMsgMS;
    }

    public SinkinCfg setTimeRateGetMissMsgMS(Integer timeRateGetMissMsgMS) {
        this.timeRateGetMissMsgMS = timeRateGetMissMsgMS;
        return this;
    }

    public Integer getTimeRateGetLatestMsgMS() {
        return timeRateGetLatestMsgMS;
    }

    public SinkinCfg setTimeRateGetLatestMsgMS(Integer timeRateGetLatestMsgMS) {
        this.timeRateGetLatestMsgMS = timeRateGetLatestMsgMS;
        return this;
    }

    public Integer getZmqSubBufferSize() {
        return zmqSubBufferSize;
    }

    public SinkinCfg setZmqSubBufferSize(Integer zmqSubBufferSize) {
        this.zmqSubBufferSize = zmqSubBufferSize;
        return this;
    }

    public Integer getMaxObjectsPoolWait() {
        return maxObjectsPoolWait;
    }

    public SinkinCfg setMaxObjectsPoolWait(Integer maxObjectsPoolWait) {
        this.maxObjectsPoolWait = maxObjectsPoolWait;
        return this;
    }

    public Integer getMaxTimeWaitMS() {
        return maxTimeWaitMS;
    }

    public SinkinCfg setMaxTimeWaitMS(Integer maxTimeWaitMS) {
        this.maxTimeWaitMS = maxTimeWaitMS;
        return this;
    }

    public String getQueuePath() {
        return queuePath;
    }

    public SinkinCfg setQueuePath(String queuePath) {
        this.queuePath = queuePath;
        return this;
    }

    public Integer getRealtimePort() {
        return realtimePort;
    }

    public SinkinCfg setRealtimePort(Integer realtimePort) {
        this.realtimePort = realtimePort;
        return this;
    }

    public Integer getConfirmPort() {
        return confirmPort;
    }

    public SinkinCfg setConfirmPort(Integer confirmPort) {
        this.confirmPort = confirmPort;
        return this;
    }

    public String getSourceIP() {
        return sourceIP;
    }

    public SinkinCfg setSourceIP(String sourceIP) {
        this.sourceIP = sourceIP;
        return this;
    }

    public String getRealTimeUrl() {
        return MessageFormat.format("tcp://{0}:{1}", sourceIP, realtimePort + "");
    }

    public String getConfirmUrl() {
        return MessageFormat.format("tcp://{0}:{1}", sourceIP, confirmPort + "");
    }

}
