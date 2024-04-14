package io.github.vuhoangha.ManyToOne;

import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;

import java.text.MessageFormat;

public class CollectorCfg {

    // đường dẫn tới folder chứa dữ liệu
    private String queuePath;

    // port mà Collector lắng nghe các req của Snipper
    private Integer port;

    // tên của người đọc. Nó sẽ dùng làm ID để sau khi restart, ta sẽ tiếp tục đọc trong queue chứ ko phải đọc từ đầu
    private String readerName;

    // bắt đầu đọc queue từ index nào. Nếu bằng "-1" thì nó sẽ đọc lại từ đầu
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


    public CollectorCfg() {
    }

    public static CollectorCfg builder() {
        return new CollectorCfg();
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
