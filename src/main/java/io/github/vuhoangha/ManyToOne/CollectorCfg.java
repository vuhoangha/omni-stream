package io.github.vuhoangha.ManyToOne;

import io.github.vuhoangha.Common.OmniWaitStrategy;
import io.github.vuhoangha.OneToMany.SinkinCfg;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;

import java.text.MessageFormat;

public class CollectorCfg {

    // đường dẫn tới folder chứa dữ liệu
    // required
    private String queuePath;

    // port mà Collector lắng nghe các request của Snipper
    private Integer port;

    // tên của người đọc. Nó sẽ dùng làm ID để sau khi restart, ta sẽ tiếp tục đọc từ vị trí cũ trong queue chứ ko phải đọc từ đầu
    // required
    private String readerName;

    // bắt đầu đọc queue từ index nào. Nếu bằng "-1" thì nó sẽ đọc từ đầu
    private Long startId;

    /*
     * thời gian định kỳ đóng file cũ, tạo file mới trong queue
     * mặc định đang để LargeRollCycles.LARGE_DAILY
     *      trong chronicle queue sẽ dùng 1 số long 8 byte = 64 bit để đánh index
     *      1 index sẽ gồm 2 phần là [cycle number][sequence in cycle number]
     *      với LARGE_DAILY, "cycle number" sẽ gồm 27 bit, trừ 1 bit chứa dấu âm dương --> tối đa 2^26=67,108,864 chu kì --> 183,859 năm
     *          "sequence in cycle number" sẽ gồm 37 bit --> "137,438,953,471" item 1 cycle --> 1,590,728 item / giây
     */
    private LargeRollCycles rollCycles;

    // kiểu WaitStrategy được sử dụng lắng nghe các item mới được ghi vào queue
    private OmniWaitStrategy queueWaitStrategy;

    // cho phép ZMQ Router chạy trên 1 CPU core riêng ?
    private Boolean enableZRouterBindingCore;

    /*
     * cho phép ZMQ Router chạy trên 1 logical processor riêng ?
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer zRouterCpu;

    // cho phép Listener Queue chạy trên 1 CPU core riêng ?
    private Boolean enableQueueBindingCore;

    /*
     * cho phép Listener Queue chạy trên 1 logical processor riêng ?
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer queueCpu;

    // cho phép toàn bộ Collector chạy chung trên 1 CPU core riêng ?
    private Boolean enableBindingCore;

    /*
     * cho phép Collector chạy chung trên 1 logical processor riêng ?
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer cpu;


    public CollectorCfg() {
    }

    public static CollectorCfg builder() {
        return new CollectorCfg();
    }


    public Integer getCpu() {
        return cpu;
    }

    public CollectorCfg setCpu(Integer cpu) {
        this.cpu = cpu;
        return this;
    }

    public Boolean getEnableBindingCore() {
        return enableBindingCore;
    }

    public CollectorCfg setEnableBindingCore(Boolean enableBindingCore) {
        this.enableBindingCore = enableBindingCore;
        return this;
    }

    public Integer getZRouterCpu() {
        return zRouterCpu;
    }

    public CollectorCfg setZRouterCpu(Integer zRouterCpu) {
        this.zRouterCpu = zRouterCpu;
        return this;
    }

    public Boolean getEnableZRouterBindingCore() {
        return enableZRouterBindingCore;
    }

    public CollectorCfg setEnableZRouterBindingCore(Boolean enableZRouterBindingCore) {
        this.enableZRouterBindingCore = enableZRouterBindingCore;
        return this;
    }

    public Integer getQueueCpu() {
        return queueCpu;
    }

    public CollectorCfg setQueueCpu(Integer queueCpu) {
        this.queueCpu = queueCpu;
        return this;
    }

    public Boolean getEnableQueueBindingCore() {
        return enableQueueBindingCore;
    }

    public CollectorCfg setEnableQueueBindingCore(Boolean enableQueueBindingCore) {
        this.enableQueueBindingCore = enableQueueBindingCore;
        return this;
    }

    public OmniWaitStrategy getQueueWaitStrategy() {
        return queueWaitStrategy;
    }

    public CollectorCfg setQueueWaitStrategy(OmniWaitStrategy queueWaitStrategy) {
        this.queueWaitStrategy = queueWaitStrategy;
        return this;
    }

    public LargeRollCycles getRollCycles() {
        return rollCycles;
    }

    public CollectorCfg setRollCycles(LargeRollCycles rollCycles) {
        this.rollCycles = rollCycles;
        return this;
    }

    public Long getStartId() {
        return startId;
    }

    public CollectorCfg setStartId(Long startId) {
        this.startId = startId;
        return this;
    }

    public String getReaderName() {
        return readerName;
    }

    public CollectorCfg setReaderName(String readerName) {
        this.readerName = readerName;
        return this;
    }

    public String getQueuePath() {
        return queuePath;
    }

    public CollectorCfg setQueuePath(String queuePath) {
        this.queuePath = queuePath;
        return this;
    }

    public String getUrl() {
        return MessageFormat.format("tcp://*:{0}", port + "");
    }

    public Integer getPort() {
        return port;
    }

    public CollectorCfg setPort(Integer port) {
        this.port = port;
        return this;
    }

}
