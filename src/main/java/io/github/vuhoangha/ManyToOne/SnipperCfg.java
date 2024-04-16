package io.github.vuhoangha.ManyToOne;

import com.lmax.disruptor.WaitStrategy;

import java.text.MessageFormat;

public class SnipperCfg {

    // IP của Collector
    private String collectorIP;

    // timeout gửi nhận message
    private Integer timeout;

    // kiểu WaitStrategy được sử dụng để gom message từ nhiều thread để dùng ZeroMQ gửi qua Collector
    private WaitStrategy waitStrategy;

    // port mà Collector lắng nghe
    private Integer port;

    // kích cỡ ring buffer của disruptor gửi/nhận message. Phải là dạng 2^n
    private Integer ringBufferSize;


    public SnipperCfg() {
    }

    public static SnipperCfg builder() {
        return new SnipperCfg();
    }


    public Integer getRingBufferSize() {
        return ringBufferSize;
    }

    public SnipperCfg setRingBufferSize(Integer ringBufferSize) {
        this.ringBufferSize = ringBufferSize;
        return this;
    }

    public String getUrl(){
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

    public Integer getTimeout() {
        return timeout;
    }

    public SnipperCfg setTimeout(Integer timeout) {
        this.timeout = timeout;
        return this;
    }

}
