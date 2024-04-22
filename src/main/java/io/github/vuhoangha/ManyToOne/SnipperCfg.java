package io.github.vuhoangha.ManyToOne;

import com.lmax.disruptor.WaitStrategy;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import io.github.vuhoangha.OneToMany.SinkinCfg;

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


    public SnipperCfg() {
    }

    public static SnipperCfg builder() {
        return new SnipperCfg();
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
