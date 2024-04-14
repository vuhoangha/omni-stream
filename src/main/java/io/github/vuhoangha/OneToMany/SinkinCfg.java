package io.github.vuhoangha.OneToMany;

import com.lmax.disruptor.WaitStrategy;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;

import java.text.MessageFormat;

public class SinkinCfg {

    // folder chứa data của queue
    private String queuePath;

    // IP của source
    private String sourceIP;

    // port của socket publish msg sang sink
    private Integer realtimePort;

    // port của socket để sink call sang src
    private Integer confirmPort;

    // 1 msg ở trong hàng chờ lâu hơn config này sẽ được lấy các msg thiếu ở giữa ["index được xử lý gần nhất", "msg có seq thấp nhất đang chờ xử lý"]
    private Integer maxTimeWaitMS;

    // số lượng msg tối đa trong ObjectsPool dùng cho việc chứa lại các msg chờ xử lý
    private Integer maxObjectsPoolWait;

    // kích cỡ tối đa bộ đệm socket SUB chờ được xử lý (số lượng msg)
    private Integer zmqSubBufferSize;

    // thời gian định kỳ lấy msg mới nhất từ src (miliseconds)
    private Integer timeRateGetLatestMsgMS;

    // thời gian định kỳ check và lấy các msg bị thiếu
    private Integer timeRateGetMissMsgMS;

    // timeout send, receive khi lấy req miss của src
    private Integer timeoutSendReqMissMsg;

    // timeout send, receive khi lấy req miss của src
    private Integer timeoutRecvReqMissMsg;

    // kiểu WaitStrategy được sử dụng để gom msg lại và xử lý
    private WaitStrategy waitStrategy;

    // kích cỡ ring buffer của disruptor xử lý msg. Phải là dạng 2^n
    private Integer ringBufferSize;

    /*
     * thời gian định kỳ đóng file cũ, tạo file mới trong queue
     * mặc định đang để LargeRollCycles.LARGE_DAILY
     *      trong chronicle queue sẽ dùng 1 số long 8 byte = 64 bit để đánh index
     *      1 index sẽ gồm 2 phần là [cycle number][sequence in cycle number]
     *      với LARGE_DAILY, "cycle number" sẽ gồm 27 bit, trừ 1 bit chứa dấu âm dương --> tối đa 2^26=67,108,864 chu kì --> 183,859 năm
     *          "sequence in cycle number" sẽ gồm 37 bit --> "137,438,953,471" item 1 cycle --> 1,590,728 item / giây
     */
    private LargeRollCycles rollCycles;


    public SinkinCfg() {
    }

    public static SinkinCfg builder() {
        return new SinkinCfg();
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
