# OmniStream

![Logo OmniStream](https://github.com/vuhoangha/kendrick-store-public/blob/main/kendrick_library_logo_128.png?raw=true)

**OmniStream** is a high-performance library designed to seamlessly synchronize data queues across multiple hosts over a
network. Inspired by the seamless flow of the river and the all-encompassing journey in Hermann Hesse's *Siddhartha*,
OmniStream offers a robust solution for real-time data streaming and synchronization with an emphasis on simplicity and
integrity.

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

### Installation

To include OmniStream in your project, add the following dependency to your project's build file:

#### Maven

```xml

<dependency>
    <groupId>io.github.vuhoangha</groupId>
    <artifactId>omni-stream</artifactId>
    <version>1.0.1</version>
</dependency>
```

#### Gradle

```groovy
dependencies {
    implementation 'io.github.vuhoangha:omni-stream:1.0.1'
}
```

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

    try {
      new Sinkin(
              SinkinCfg.builder()
                      .setQueuePath(sinkPath)
                      .setSourceIP("127.0.0.1"),
              PeopleTest.class,
              handler);
    } catch (Exception ex) {
    }
  }


  public static void runSource() {
    try {
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
    } catch (Exception ex) {

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
        new Thread(() -> ManyToOneExample.runSnipper(5557, 1)).start();
        LockSupport.parkNanos(500_000_000L);
        new Thread(() -> ManyToOneExample.runSnipper(5557, 100000)).start();
    }


    public static void runCollector() {
        try {
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
        } catch (Exception ex) {
        }
    }


    public static void runSnipper(int port, int startIndex) {
        try {
            Snipper<PeopleTest> snipper = new Snipper<>(SnipperCfg.builder().setCollectorIP("localhost").setPort(port));
            int count = startIndex;
            while (true) {
                PeopleTest people = new PeopleTest(count, "people " + count);
                System.out.println("\n\uD83D\uDE80Send: " + people);
                snipper.send(people);

                count++;
                LockSupport.parkNanos(1_000_000_000);
            }
        } catch (Exception ex) {
        }
    }


}
```

### How does it work?
![Diagram](https://github.com/vuhoangha/kendrick-store-public/blob/main/Fanout_sinkin.png?raw=true)
![Diagram](https://github.com/vuhoangha/kendrick-store-public/blob/main/Snipper_collector.png?raw=true)

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

For support, contact [support@omnistream.com](mailto:support@omnistream.com).
