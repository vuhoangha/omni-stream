# OmniStream

![Logo OmniStream](https://github.com/vuhoangha/kendrick-store-public/blob/main/kendrick_library_logo_128.png?raw=true)

**OmniStream** is a high-performance library designed to seamlessly synchronize data queues across multiple hosts over a
network. Inspired by the seamless flow of the river and the all-encompassing journey in Hermann Hesse's *Siddhartha*,
OmniStream offers a robust solution for real-time data streaming and synchronization with an emphasis on simplicity and
integrity.

### Installation

To include OmniStream in your project, add the following dependency to your project's build file:

#### Maven

```xml

<dependency>
    <groupId>io.github.vuhoangha</groupId>
    <artifactId>omni-stream</artifactId>
    <version>1.0.2</version>
</dependency>
```

#### Gradle

```groovy
dependencies {
    implementation 'io.github.vuhoangha:omni-stream:1.0.2'
}
```

## Features

- **High Throughput**: Utilizes Chronicle Queue, LMAX Disruptor, and ZeroMQ to ensure high-speed data processing and low
  latency.
- **Scalability**: Effortlessly scales across multiple nodes to accommodate growing data demands.
- **Resilience**: Built to handle failures gracefully, ensuring continuous data availability.
- **Simplicity**: Easy to set up and integrate into existing infrastructures with minimal configuration.

## Getting Started

### Prerequisites

Ensure you have the following installed:

- Java 8 or higher
- Maven or Gradle

### Java 11 and Java 17 Support

#### Overview

OmniStream is compatible with Java 8, Java 11, and the latest long-term support version, Java 17. It is possible to run
all new releases under Java 17 on the classpath (not yet under the module path).

#### Strongly Encapsulated JDK Internals

Java 17 introduces "Strongly Encapsulated JDK Internals" (JEP 403), enhancing the robustness and security of the
execution environment. Since Chronicle libraries leverage a tightly integrated subset of JDK internals for performance,
certain adjustments are necessary when migrating to Java 17. These adjustments are also recommended when running under
Java 11 to avoid warnings.

#### Command Line Parameters

For applications explicitly started with the `java` command, the following command line parameters need to be included
to ensure compatibility with Java 11 and Java 17:

```
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED
--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

These parameters can often be included via the environment variable `JAVA_OPTS`.

#### Maven Integration

If you are using Maven, such as with the `exec-maven-plugin` to run Java applications, set the `MAVEN_OPTS` environment
variable as follows:

```
export MAVEN_OPTS="--add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED"
```

#### Recommendation for Java 11

Although only a subset of the above command line options is required for Java 11, we recommend applying the same
parameters to Java 11 as to Java 17 to eliminate certain warnings that may appear in output logs.

### Quick Start

Here's a simple example to get you started with OmniStream:

- Data type for testing

```java
public class PeopleTest extends SelfDescribingMarshallable {
    
    private int index;
    private String name;

    // Constructors
    public PeopleTest() {
    }

    public PeopleTest(int index, String name) {
        this.index = index;
        this.name = name;
    }

    // Getters and setters
    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "People{" +
                "index=" + index +
                ", name='" + name + '\'' +
                '}';
    }
}
```

- Fanout/Sinkin
```java
public class OneToManyExample {


  public static String sourcePath = "xxx";
  public static String sinkPath = "zzz";


  public static void run() {
    new Thread(OneToManyExample::runSource).start();
    LockSupport.parkNanos(2_000_000_000L);
    new Thread(OneToManyExample::runSink).start();
  }


  public static void runSink() {
    SinkinHandler<PeopleTest> handler = (byte version, PeopleTest data, long seq, long id) -> {
      System.out.println("\uD83D\uDCE9Received");
      System.out.println("Version: " + version);
      System.out.println("People: " + data.toString());
      System.out.println("Seq: " + seq);
      System.out.println("ID: " + id);
    };

    new Sinkin(
            SinkinCfg.builder()
                    .setQueuePath(sinkPath)
                    .setSourceIP("127.0.0.1"),
            PeopleTest.class,
            handler);
  }


  public static void runSource() {
    Fanout<PeopleTest> fanout = new Fanout<>(
            FanoutCfg.builder().setQueuePath(sourcePath),
            PeopleTest.class);

    PeopleTest people = new PeopleTest();
    int count = 0;
    while (true) {
      count++;
      people.setIndex(count);
      people.setName("people " + count);
      System.out.println("\n\uD83D\uDE80Send: " + people);
      fanout.write(people);

      LockSupport.parkNanos(2_000_000_000L);
    }
  }

}
```

- Snipper/Collector
```java
public class ManyToOneExample {

  public static String collectorPath = "xxx";

  public static void run() {
    new Thread(ManyToOneExample::runCollector).start();
    LockSupport.parkNanos(2_000_000_000L);
    new Thread(() -> ManyToOneExample.runSnipper(1)).start();
    LockSupport.parkNanos(500_000_000L);
    new Thread(() -> ManyToOneExample.runSnipper(100000)).start();
  }


  public static void runCollector() {
    new Collector<>(
            CollectorCfg.builder()
                    .setQueuePath(collectorPath)
                    .setReaderName("reader_name"),
            PeopleTest.class,
            (people, index) -> {
              System.out.println("\uD83D\uDCE9Received");
              System.out.println("index: " + index);
              System.out.println("people: " + people);
            }
    );
  }


  public static void runSnipper(int startIndex) {
    Snipper<PeopleTest> snipper = new Snipper<>(SnipperCfg.builder().setCollectorIP("localhost"));
    int count = startIndex;
    while (true) {
      PeopleTest people = new PeopleTest(count, "people " + count);
      System.out.println("\n\uD83D\uDE80Send: " + people);
      snipper.send(people);

      count++;
      LockSupport.parkNanos(1_000_000_000);
    }
  }


}
```

### How does it work?
#### Fanout / Sinkin (One to Many)
![Diagram](https://github.com/vuhoangha/kendrick-store-public/blob/main/Fanout_sinkin.png?raw=true)

##### Overview
This pattern provides a robust solution for synchronizing a queue from one host (Fanout) to multiple other hosts (Sinkin) using the Fanout/Sinkin pattern. It guarantees the order of queue items and ensures exact replication across hosts.

##### Components

##### Fanout Pattern
The Fanout class is responsible for distributing data across multiple hosts. It operates as follows:

- **Data Input**: Threads from the main application send data to an Lmax Disruptor, which acts as a high-performance, inter-thread messaging library.
- **Data Storage**: The Disruptor writes this data into a Chronicle Queue, a low-latency, high-throughput, persisted queue.
- **Data Distribution**: A dedicated processor listens for new messages written to the Chronicle Queue and forwards them via ZeroMQ to other hosts (Sinkin) for synchronization.

##### Sinkin Pattern
The Sinkin class handles receiving messages from the Fanout host and ensures they are processed correctly:

- **Data Subscription**: Uses ZeroMQ to subscribe to new messages sent by the Fanout.
- **Data Recording**: Records these messages into a Chronicle Queue on the local host.
- **Data Processing**: A processor listens for new messages added to the local queue and forwards them to the application for further processing.

##### Data Integrity and Synchronization

- **Message Delivery**: While the pub/sub model facilitates real-time data distribution, message loss can occur under certain conditions.
- **Reliability Mechanism**: To address potential data loss, an additional ZeroMQ Req/Rep mechanism is implemented. This mechanism checks for any missed messages and synchronizes them accordingly, ensuring complete data integrity across all hosts.

##### Configuration

##### Fanout Config
- `queuePath *`
  > Configures the directory that will contain the queue data on disk.
  > Example: `/var/lib/yourapp/queue`

- `realtimePort`
  > Port used by ZeroMQ to listen for connections for publishing/subscribing to new messages.
  > Default: `5555`

- `confirmPort`
  > Port used by ZeroMQ to listen for connections to retrieve missed messages.
  > Default: `5556`

- `numberMsgInBatch`
  > Specifies the maximum number of messages sent in one request when a Sinkin starts. This setting balances the number of requests with potential data transmission interruptions.
  > Default: `10,000`

- `disruptorWaitStrategy`
  > The wait strategy of the Lmax Disruptor for batching messages from multiple threads. Consider the trade-off between latency and CPU performance based on your usage needs.
  > Default: `YieldingWaitStrategy`
  > More info: [Lmax Disruptor User Guide](https://lmax-exchange.github.io/disruptor/user-guide/index.html)

- `ringBufferSize`
  > Size of the Lmax Disruptor's Ring Buffer for sending/receiving messages. Must be a power of two.
  > Default: `131,072`

- `queueWaitStrategy`
  > The wait strategy of the Processor for new messages from Chronicle Queue.
  > Default: `OmniWaitStrategy.YIELD`

- `maxNumberMsgInCachePub`
  > Maximum number of messages in the ZeroMQ publisher's cache.
  > Default: `1,000,000`

- `version`
  > Version identifier for the messages currently in the queue, used for future checks and validations.
  > Default: `-128`

- `rollCycles`
  > Frequency at which the Chronicle Queue rolls over from an old file to a new file.
  > Default: `LargeRollCycles.LARGE_DAILY` (daily rollover)
  > More info: [Chronicle Queue FAQ](https://github.com/OpenHFT/Chronicle-Queue/blob/ea/docs/FAQ.adoc#how-to-change-the-time-that-chronicle-queue-rolls)

- `enableBindingCore`
  > Allows the entire Fanout process to run on a dedicated CPU core.
  > Default: `false`

- `cpu`
  > Specifies the logical processor for the Fanout process:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
  > Default: `Constance.CPU_TYPE.ANY`

- `enableDisruptorBindingCore`
  > Allows the Lmax Disruptor to run on a dedicated CPU core.
  > Default: `false`

- `disruptorCpu`
  > Specifies the logical processor for the Fanout process:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
      > Default: `Constance.CPU_TYPE.NONE`

- `enableQueueBindingCore`
  > Allows the Chronicle Queue to run on a dedicated CPU core.
  > Default: `false`

- `queueCpu`
  > Specifies the logical processor for the Fanout process:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
      > Default: `Constance.CPU_TYPE.NONE`

- `enableHandleConfirmBindingCore`
  > Allows the confirm handler to run on a dedicated CPU core.
  > Default: `false`

- `handleConfirmCpu`
  > Specifies the logical processor for the Fanout process:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
      > Default: `Constance.CPU_TYPE.NONE`

##### Sinkin Config
- `queuePath *`
  > Specifies the directory that will contain the queue data on disk.
  > Example: `/var/lib/yourapp/queue`

- `sourceIP *`
  > IP address of the Fanout host from which the Sinkin receives data.

- `realtimePort`
  > Port used by the Fanout to publish data to the Sinkins.
  > Default: `5555`

- `confirmPort`
  > Port used by the Fanout to respond to requests for retrieving missed messages from Sinkins.
  > Default: `5556`

- `maxTimeWaitMS`
  > Maximum time a message can stay in the queue before it is considered as missing preceding messages, prompting a request to the Fanout to retrieve them.
  > Default: `1000` ms

- `maxObjectsPoolWait`
  > Maximum number of messages in the ObjectsPool waiting to be processed.
  > Default: `30,000`

- `zmqSubBufferSize`
  > Maximum size of the ZeroMQ SUB buffer for pending messages.
  > Default: `1,000,000`

- `timeRateGetLatestMsgMS`
  > Frequency in milliseconds to fetch the latest messages from the Fanout.
  > Default: `3000` ms

- `timeRateGetMissMsgMS`
  > Frequency in milliseconds to check and retrieve missing messages.
  > Default: `3000` ms

- `timeoutSendReqMissMsg`
  > Timeout for sending requests to retrieve missing messages from the Fanout.
  > Default: `5000` ms

- `timeoutRecvReqMissMsg`
  > Timeout for receiving responses for missing messages from the Fanout.
  > Default: `5000` ms

- `waitStrategy`
  > The wait strategy used by Lmax Disruptor for batching and processing messages.
  > Default: `BlockingWaitStrategy`
  > More info: [Lmax Disruptor User Guide](https://lmax-exchange.github.io/disruptor/user-guide/index.html)

- `ringBufferSize`
  > Size of the Disruptor's ring buffer for message processing. Must be a power of two.
  > Default: `131,072`

- `rollCycles`
  > Period for rolling over from an old file to a new file in the queue. The `LargeRollCycles.LARGE_DAILY` setting provides a balance between indexing and file management.
  > Default: `LargeRollCycles.LARGE_DAILY`
  > More info: [Chronicle Queue FAQ](https://github.com/OpenHFT/Chronicle-Queue/blob/ea/docs/FAQ.adoc#how-to-change-the-time-that-chronicle-queue-rolls)

- `enableBindingCore`
  > Allows the entire Sinkin process to run on a dedicated CPU core.
  > Default: `false`

- `cpu`
  > Specifies the logical processor for the Sinkin process:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
  > Default: `Constance.CPU_TYPE.ANY`

- `enableDisruptorProcessMsgBindingCore`
  > Allows the Disruptor message processing to run on a dedicated CPU core.
  > Default: `false`

- `disruptorProcessMsgCpu`
  > Specifies the logical processor for the Disruptor message processing:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
  > Default: `Constance.CPU_TYPE.NONE`

- `enableCheckMissMsgAndSubQueueBindingCore`
  > Allows the process that checks for missed messages and subscribes to new items in the queue to run on a dedicated CPU core.
  > Default: `false`

- `checkMissMsgAndSubQueueCpu`
  > Specifies the logical processor for checking missed messages and subscribing to new queue items:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
  > Default: `Constance.CPU_TYPE.NONE`

- `enableSubMsgBindingCore`
  > Allows the process for subscribing to new messages to run on a dedicated CPU core.
  > Default: `false`

- `subMsgCpu`
  > Specifies the logical processor for subscribing to new messages:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
  > Default: `Constance.CPU_TYPE.NONE`

- `queueWaitStrategy`
  > The wait strategy of the Processor for new messages from Chronicle Queue.
  > Default: `OmniWaitStrategy.YIELD`
  

#### Snipper / Collector (Many to One)
![Diagram](https://github.com/vuhoangha/kendrick-store-public/blob/main/Snipper_collector.png?raw=true)

##### Overview
This pattern designed to streamline the process of aggregating messages from multiple hosts into a single Collector host. It ensures efficient and reliable message delivery into a central queue for processing.

##### Components

##### Snipper Pattern
The Snipper class is tasked with capturing and forwarding data from various sources. Its operations are as follows:

- **Data Input**: Threads from the main application send data to an Lmax Disruptor, which serves as a high-performance inter-thread messaging platform.
- **Data Transmission**: The Disruptor sends this data to the Collector host via ZeroMQ.

##### Collector Pattern
The Collector class handles the centralized collection and processing of messages:

- **Data Reception**: Utilizes ZeroMQ to listen for incoming messages from Snippers.
- **Data Recording**: Upon receiving messages, they are recorded into a Chronicle Queue on the Collector host.
- **Data Processing**: A dedicated processor listens for new messages in the queue and forwards them to the application for further processing.

##### Message Delivery and Integrity

- **Acknowledgment Mechanism**: The communication between Snippers and the Collector includes an acknowledgment (ACK) protocol. The Collector, upon receiving a message, notifies the respective Snipper, confirming receipt.
- **Loss Handling**: In case of a message timeout or failure, the Snipper marks the message as undelivered, allowing for corrective actions to be taken.
- **Data Replay**: The Collector can perform data replays from past records, enabling recovery and continuity in case of disruptions.

##### Configuration

##### Collector Config
- `queuePath *`
  > Path to the folder containing queue data.
  > Example: `/var/lib/yourapp/queue`

- `port`
  > Port that the Collector listens on to receive requests from the Snipper.
  > Default: `5557`

- `readerName *`
  > Name used as an ID for the reader. This name allows the Collector to continue reading from the last position in the queue after a restart, rather than starting over from the beginning.
  > Example: `defaultReader`

- `startId`
  > Starting index from which the queue will be read. If set to `-1`, the reading will begin from the start of the queue.

- `queueWaitStrategy`
  > The wait strategy used for listening to new items written to the queue. OmniWaitStrategy optimizes performance by reducing CPU usage while waiting.
  > Default: `OmniWaitStrategy.YIELD`

- `enableBindingCore`
  > Allows the entire Collector process to run on a dedicated CPU core.
  > Default: `false`

- `cpu`
  > Specifies the logical processor for the entire Collector process:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
  > Default: `Constance.CPU_TYPE.ANY`

- `enableZRouterBindingCore`
  > Allows the ZeroMQ Router to run on a dedicated CPU core.
  > Default: `false`

- `zRouterCpu`
  > Specifies the logical processor for the ZeroMQ Router:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
  > Default: `Constance.CPU_TYPE.NONE`

- `enableQueueBindingCore`
  > Allows the Queue Listener to run on a dedicated CPU core.
  > Default: `false`

- `queueCpu`
  > Specifies the logical processor for the Queue Listener:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
  > Default: `Constance.CPU_TYPE.NONE`


##### Snipper Config
- `collectorIP *`
  > IP address of the Collector host to which the Snipper sends data.

- `timeout`
  > Timeout for sending and receiving messages.
  > Default: `10,000` ms

- `waitStrategy`
  > The wait strategy used by Lmax Disruptor to batch messages from multiple threads before sending them via ZeroMQ to the Collector. This setting balances throughput and system responsiveness.
  > Default: `BlockingWaitStrategy`
  > More info: [Lmax Disruptor User Guide](https://lmax-exchange.github.io/disruptor/user-guide/index.html)

- `port`
  > Port that the Collector listens on to receive messages from the Snipper.
  > Default: `5557`

- `ringBufferSize`
  > Size of the Disruptor's ring buffer for sending/receiving messages. Must be a power of two to ensure efficient data handling.
  > Default: `131,072`

- `disruptorWaitStrategy`
  > The wait strategy of the Processor for new messages from application.
  > Default: `OmniWaitStrategy.YIELD`

- `enableDisruptorBindingCore`
  > Allows the Disruptor message processing to run on a dedicated CPU core.
  > Default: `false`

- `disruptorCpu`
  > Specifies the logical processor for the Disruptor message processing:
  > - `Constance.CPU_TYPE.ANY`: Runs on any available logical processor, prioritizing isolated ones if available.
  > - `Constance.CPU_TYPE.NONE`: Runs on multiple logical processors as managed by the operating system.
  > - `>= 0`: Runs on specifies the logical processor.
      > Default: `Constance.CPU_TYPE.ANY` 

  
## Documentation

For detailed documentation, examples, and API references, please
visit [OmniStream Documentation](https://github.com/vuhoangha/omni-stream).

## Contributing

We welcome contributions from the community! If you'd like to contribute, please follow
our [contributing guidelines](CONTRIBUTING.md).

## License

OmniStream is released under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Acknowledgments

- Inspired by Hermann Hesse's philosophical exploration in *Siddhartha*.
- Thanks to the open source community for continuous support and inspiration.

---

For support, contact [vuhoangha100995@gmail.com](mailto:vuhoangha100995@gmail.com).
