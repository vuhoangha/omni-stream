package io.github.vuhoangha.ManyToOne;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import io.github.vuhoangha.OneToMany.SinkinCfg;

import java.text.MessageFormat;

public class SnipperCfg {

    // IP của Collector
    private String collectorIP;

    // timeout gửi nhận message dành cho user
    private Long timeout;

    // thời gian message còn hiệu lực khi Collector nhận được
    // 'ttl' nên nhỏ hơn 'timeout' để chắc chắn rằng message này không thể được xử lý bởi Collector
    private Long ttl;

    // kiểu WaitStrategy được sử dụng để gom message từ nhiều thread để dùng ZeroMQ gửi qua Collector
    private WaitStrategy waitStrategy;

    // port mà Collector lắng nghe
    private Integer port;

    // port mà Collector gửi lại Time Server
    private Integer timePort;

    // kích cỡ ring buffer của disruptor gửi/nhận message. Phải là dạng 2^n
    private Integer ringBufferSize;

    // kiểu WaitStrategy được sử dụng để lắng nghe các msg yêu cầu gửi đi
    private OmniWaitStrategy disruptorWaitStrategy;

    // cho phép disruptor chạy trên 1 CPU core riêng ?
    private Boolean enableDisruptorBindingCore;

    /*
     * cho phép disruptor chạy trên 1 logical processor riêng ?
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer disruptorCpu;

    // bao lâu đồng bộ time server 1 lần
    private Long syncTimeServerInterval;


    private SnipperCfg() {
    }

    public static SnipperCfg builder() {
        SnipperCfg cfg = new SnipperCfg();

        cfg.setPort(5557);
        cfg.setTimePort(5558);
        cfg.setTimeout(30000L);
        cfg.setTtl(15000L);
        cfg.setWaitStrategy(new BlockingWaitStrategy());
        cfg.setRingBufferSize(2 << 16);     // 131072
        cfg.setDisruptorWaitStrategy(OmniWaitStrategy.YIELD);
        cfg.setDisruptorCpu(Constance.CPU_TYPE.ANY);
        cfg.setEnableDisruptorBindingCore(false);
        cfg.setSyncTimeServerInterval(5000L);

        return cfg;
    }


    public Long getSyncTimeServerInterval() {
        return syncTimeServerInterval;
    }

    public SnipperCfg setSyncTimeServerInterval(Long syncTimeServerInterval) {
        this.syncTimeServerInterval = syncTimeServerInterval;
        return this;
    }

    public Integer getTimePort() {
        return timePort;
    }

    public SnipperCfg setTimePort(Integer timePort) {
        this.timePort = timePort;
        return this;
    }

    public String getTimeServerUrl() {
        return MessageFormat.format("tcp://{0}:{1}", collectorIP, timePort + "");
    }

    public Integer getDisruptorCpu() {
        return disruptorCpu;
    }

    public SnipperCfg setDisruptorCpu(Integer disruptorCpu) {
        this.disruptorCpu = disruptorCpu;
        return this;
    }

    public Boolean getEnableDisruptorBindingCore() {
        return enableDisruptorBindingCore;
    }

    public SnipperCfg setEnableDisruptorBindingCore(Boolean enableDisruptorBindingCore) {
        this.enableDisruptorBindingCore = enableDisruptorBindingCore;
        return this;
    }

    public OmniWaitStrategy getDisruptorWaitStrategy() {
        return disruptorWaitStrategy;
    }

    public SnipperCfg setDisruptorWaitStrategy(OmniWaitStrategy disruptorWaitStrategy) {
        this.disruptorWaitStrategy = disruptorWaitStrategy;
        return this;
    }

    public Integer getRingBufferSize() {
        return ringBufferSize;
    }

    public SnipperCfg setRingBufferSize(Integer ringBufferSize) {
        this.ringBufferSize = ringBufferSize;
        return this;
    }

    public String getUrl() {
        return MessageFormat.format("tcp://{0}:{1}", collectorIP, port + "");
    }

    public Integer getPort() {
        return port;
    }

    public SnipperCfg setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getCollectorIP() {
        return collectorIP;
    }

    public SnipperCfg setCollectorIP(String ip) {
        this.collectorIP = ip;
        return this;
    }

    public WaitStrategy getWaitStrategy() {
        return waitStrategy;
    }

    public SnipperCfg setWaitStrategy(WaitStrategy waitStrategy) {
        this.waitStrategy = waitStrategy;
        return this;
    }

    public Long getTimeout() {
        return timeout;
    }

    public SnipperCfg setTimeout(Long timeout) {
        this.timeout = timeout;
        return this;
    }

    public Long getTtl() {
        return ttl;
    }

    public SnipperCfg setTtl(Long ttl) {
        this.ttl = ttl;
        return this;
    }

}
