package io.github.vuhoangha.OneToManyStateless;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import io.github.vuhoangha.Common.Constance;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Getter
@Setter
@Accessors(chain = true)
public class ArtemisCfg {

    // IP của Fanout host
    private String sourceIP;

    // port của Fanout dùng để publish dữ liệu cho các Sinkin
    private List<Integer> realtimePorts;

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
    private Boolean enableCheckMissMsgBindingCore;

    /*
     * Check các msg bị miss và sub các item mới trong queue msg chạy trên 1 logical processor riêng
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer checkMissMsgCpu;

    // cho phép sub các msg mới chạy trên 1 CPU core riêng ?
    private Boolean enableSubMsgBindingCore;

    /*
     * Sub các msg mới chạy trên 1 logical processor riêng
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer subMsgCpu;

    // số lượng msg tối đa 1 lần lấy với việc lấy msg FROM-TO
    private Integer batchSizeFromTo;

    // nếu 1 msg đã nằm quá lâu trong hàng đợi chứng tỏ đã miss các msg trước đó, cần restart lại
    private Integer timeoutMustResyncMs;

    // thời gian để starting là bao lâu. Đây là lúc ban đầu service sẽ ngưng xử lý các msg nhận được mà chỉ tích trữ nó lại
    // sau đó nó sẽ chọn ra msg có seq thấp nhất để làm điểm khởi đầu
    private Integer startingTimeMs;


    public ArtemisCfg() {
    }

    public static ArtemisCfg getDefault() {
        ArtemisCfg config = new ArtemisCfg();

        config.setRealtimePorts(Arrays.asList(5570));
        config.setConfirmPort(5571);
        config.setMaxTimeWaitMS(2000);
        config.setMaxObjectsPoolWait(30000);
        config.setZmqSubBufferSize(1000000);
        config.setTimeRateGetLatestMsgMS(1000);
        config.setTimeRateGetMissMsgMS(1000);
        config.setTimeoutSendReqMissMsg(5000);
        config.setTimeoutRecvReqMissMsg(5000);
        config.setWaitStrategy(new BlockingWaitStrategy());
        config.setRingBufferSize(2 << 16);     // 131072
        config.setEnableBindingCore(false);
        config.setCpu(Constance.CPU_TYPE.NONE);
        config.setEnableDisruptorProcessMsgBindingCore(false);
        config.setDisruptorProcessMsgCpu(Constance.CPU_TYPE.NONE);
        config.setEnableCheckMissMsgBindingCore(false);
        config.setCheckMissMsgCpu(Constance.CPU_TYPE.NONE);
        config.setEnableSubMsgBindingCore(false);
        config.setSubMsgCpu(Constance.CPU_TYPE.NONE);
        config.setBatchSizeFromTo(10000);
        config.setTimeoutMustResyncMs(70000);
        config.setStartingTimeMs(5000);

        return config;
    }

    public String getRealTimeUrl() {
        Random rand = new Random();
        int port = realtimePorts.get(rand.nextInt(realtimePorts.size()));
        return MessageFormat.format("tcp://{0}:{1}", sourceIP, port + "");
    }

    public String getConfirmUrl() {
        return MessageFormat.format("tcp://{0}:{1}", sourceIP, confirmPort + "");
    }

}
