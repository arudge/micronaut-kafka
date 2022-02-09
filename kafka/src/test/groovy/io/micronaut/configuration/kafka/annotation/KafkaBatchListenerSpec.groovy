package io.micronaut.configuration.kafka.annotation

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageHeader
import io.micronaut.messaging.annotation.SendTo
import io.reactivex.Flowable
import org.apache.kafka.clients.consumer.ConsumerRecord
import reactor.core.publisher.Flux

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class KafkaBatchListenerSpec extends AbstractKafkaContainerSpec {

    public static final String BOOKS_TOPIC = 'KafkaBatchListenerSpec-books'
    public static final String BOOKS_LIST_TOPIC = 'KafkaBatchListenerSpec-books-list'
    public static final String BOOKS_HEADERS_TOPIC = 'KafkaBatchListenerSpec-books-headers'
    public static final String BOOKS_FLUX_TOPIC = 'KafkaBatchListenerSpec-books-flux'
    public static final String BOOKS_FLOWABLE_TOPIC = 'KafkaBatchListenerSpec-books-flowable'
    public static final String BOOKS_FORWARD_LIST_TOPIC = 'KafkaBatchListenerSpec-books-forward-list'
    public static final String BOOKS_FORWARD_ARRAY_TOPIC = 'KafkaBatchListenerSpec-books-forward-array'
    public static final String BOOKS_FORWARD_FLUX_TOPIC = 'KafkaBatchListenerSpec-books-forward-flux'
    public static final String BOOKS_FORWARD_FLOWABLE_TOPIC = 'KafkaBatchListenerSpec-books-forward-flowable'
    public static final String BOOKS_ARRAY_TOPIC = 'KafkaBatchListenerSpec-books-array'
    public static final String TITLES_TOPIC = 'KafkaBatchListenerSpec-titles'
    public static final String BOOKS_LIST_WITH_KEYS_TOPIC = 'KafkaBatchListenerSpec-books-list-with-keys'
    public static final String BOOKS_LIST_CONSUMER_RECORDS_TOPIC = 'KafkaBatchListenerSpec-books-list-consumer-records-topic'

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                [(EMBEDDED_TOPICS): [TITLES_TOPIC,
                                     BOOKS_LIST_TOPIC,
                                     BOOKS_ARRAY_TOPIC,
                                     BOOKS_TOPIC,
                                     BOOKS_FORWARD_LIST_TOPIC]
                ]
    }

    void "test receive list consumer records"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()
        bookListener.keys?.clear()

        when:
        myBatchClient.sendBooksToListConsumerRecord([new Book(title: "The Header"), new Book(title: "The Shining")])

        then: 'expected'
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "The Header"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }

        and: 'fails with'
        def t = """16:02:53.194 [pool-1-thread-2] ERROR i.m.c.k.e.KafkaListenerExceptionHandler - Kafka consumer [io.micronaut.configuration.kafka.annotation.KafkaBatchListenerSpec$BookListener@245779dd] failed to deserialize value: Error deserializing key/value for partition KafkaBatchListenerSpec-books-list-consumer-records-topic-0 at offset 0. If needed, please seek past the record to continue consumption.
org.apache.kafka.common.errors.SerializationException: Error deserializing key/value for partition KafkaBatchListenerSpec-books-list-consumer-records-topic-0 at offset 0. If needed, please seek past the record to continue consumption.
Caused by: io.micronaut.core.serialize.exceptions.SerializationException: Error deserializing object from JSON: Cannot construct instance of `org.apache.kafka.clients.consumer.ConsumerRecord` (no Creators, like default constructor, exist): cannot deserialize from Object value (no delegate- or property-based Creator)
 at [Source: (byte[])"{"title":"The Header"}"; line: 1, column: 2]
\tat io.micronaut.json.JsonObjectSerializer.deserialize(JsonObjectSerializer.java:70)
\tat io.micronaut.configuration.kafka.serde.JsonObjectSerde.deserialize(JsonObjectSerde.java:59)
\tat org.apache.kafka.common.serialization.Deserializer.deserialize(Deserializer.java:60)
\tat org.apache.kafka.clients.consumer.internals.Fetcher.parseRecord(Fetcher.java:1386)
\tat org.apache.kafka.clients.consumer.internals.Fetcher.access\$3400(Fetcher.java:133)
\tat org.apache.kafka.clients.consumer.internals.Fetcher$CompletedFetch.fetchRecords(Fetcher.java:1617)
\tat org.apache.kafka.clients.consumer.internals.Fetcher$CompletedFetch.access\$1700(Fetcher.java:1453)
\tat org.apache.kafka.clients.consumer.internals.Fetcher.fetchRecords(Fetcher.java:686)
\tat org.apache.kafka.clients.consumer.internals.Fetcher.fetchedRecords(Fetcher.java:637)
\tat org.apache.kafka.clients.consumer.KafkaConsumer.pollForFetches(KafkaConsumer.java:1303)
\tat org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1237)
\tat org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1210)
\tat io.opentracing.contrib.kafka.TracingKafkaConsumer.poll(TracingKafkaConsumer.java:132)
\tat io.micronaut.configuration.kafka.processor.KafkaConsumerProcessor.createConsumerThreadPollLoop(KafkaConsumerProcessor.java:453)
\tat io.micronaut.configuration.kafka.processor.KafkaConsumerProcessor.lambda$submitConsumerThread\$7(KafkaConsumerProcessor.java:421)
\tat io.micronaut.scheduling.instrument.InvocationInstrumenterWrappedRunnable.run(InvocationInstrumenterWrappedRunnable.java:47)
\tat io.micrometer.core.instrument.composite.CompositeTimer.record(CompositeTimer.java:79)
\tat io.micrometer.core.instrument.Timer.lambda$wrap\$0(Timer.java:160)
\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
\tat java.base/java.lang.Thread.run(Thread.java:834)"""
    }

    void "test receive list of messages and keys"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()
        bookListener.keys?.clear()

        when:
        myBatchClient.sendBooksWithKeys(["key1", "key2"], [new Book(title: "The Header"), new Book(title: "The Shining")])

        then: 'expected'
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "The Header"))
            bookListener.books.contains(new Book(title: "The Shining"))
            bookListener.keys.size() == 2
            bookListener.keys.contains("key1")
            bookListener.keys.contains("key2")
        }

        and: 'fails with'
        def t = """
Caused by: Condition not satisfied:

  bookListener.keys.contains("key1")
  |            |    |
  |            |    false
  |            [key1,key2, key1,key2]
"""
    }

    void "test send batch list with headers - blocking"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        myBatchClient.sendBooksAndHeaders([new Book(title: "The Header"), new Book(title: "The Shining")])

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            !bookListener.headers.isEmpty()
            bookListener.headers.every() { it == "Bar" }
            bookListener.books.contains(new Book(title: "The Header"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }
    }

    void "test send batch list - blocking"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        myBatchClient.sendBooks([new Book(title: "The Stand"), new Book(title: "The Shining")])

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "The Stand"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }
    }

    void "test send and forward batch list - blocking"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        TitleListener titleListener = context.getBean(TitleListener)

        bookListener.books?.clear()

        when:
        myBatchClient.sendAndForwardBooks([new Book(title: "It"), new Book(title: "Gerald's Game")])

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "It"))
            bookListener.books.contains(new Book(title: "Gerald's Game"))
            titleListener.titles.contains("It")
            titleListener.titles.contains("Gerald's Game")
        }
    }

    void "test send batch array - blocking"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        myBatchClient.sendBooks(new Book(title: "Along Came a Spider"), new Book(title: "The Watchmen"))

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "Along Came a Spider"))
            bookListener.books.contains(new Book(title: "The Watchmen"))
        }
    }

    void "test send and forward batch array - blocking"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        TitleListener titleListener = context.getBean(TitleListener)

        bookListener.books?.clear()

        when:
        myBatchClient.sendAndForward(new Book(title: "Pillars of the Earth"), new Book(title: "War of the World"))

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "Pillars of the Earth"))
            bookListener.books.contains(new Book(title: "War of the World"))
            titleListener.titles.contains("Pillars of the Earth")
            titleListener.titles.contains("War of the World")
        }
    }

    void "test send and forward batch flux"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        Flux<Book> results = myBatchClient.sendAndForwardFlux(Flux.fromIterable([new Book(title: "The Stand"), new Book(title: "The Shining")]))
        results.collectList().block()

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "The Stand"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }
    }

    void "test send and forward batch flowable"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        Flowable<Book> results = myBatchClient.sendAndForwardFlowable(Flowable.fromIterable([new Book(title: "The Flow"), new Book(title: "The Shining")]))
        results.toList().blockingGet()

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "The Flow"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }
    }

    void "test send batch flux"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        myBatchClient.sendBooksFlux(Flux.fromIterable([new Book(title: "The Flux"), new Book(title: "The Shining")]))

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "The Flux"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }
    }

    void "test send batch flowable"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        myBatchClient.sendBooksFlowable(Flowable.fromIterable([new Book(title: "The Flowable"), new Book(title: "The Shining")]))

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "The Flowable"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchListenerSpec')
    @KafkaClient(batch = true)
    @Topic(KafkaBatchListenerSpec.BOOKS_TOPIC)
    static interface MyBatchClient {

        @Topic(KafkaBatchListenerSpec.BOOKS_LIST_TOPIC)
        void sendBooks(List<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_LIST_WITH_KEYS_TOPIC)
        void sendBooksWithKeys(@KafkaKey List<String> keys, List<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_LIST_CONSUMER_RECORDS_TOPIC)
        void sendBooksToListConsumerRecord(List<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_HEADERS_TOPIC)
        @MessageHeader(name = "X-Foo", value = "Bar")
        void sendBooksAndHeaders(List<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_LIST_TOPIC)
        void sendAndForwardBooks(List<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_ARRAY_TOPIC)
        void sendBooks(Book... books)

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_ARRAY_TOPIC)
        void sendAndForward(Book... books)

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_FLUX_TOPIC)
        Flux<Book> sendAndForwardFlux(Flux<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_FLOWABLE_TOPIC)
        Flowable<Book> sendAndForwardFlowable(Flowable<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_FLUX_TOPIC)
        void sendBooksFlux(Flux<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_FLOWABLE_TOPIC)
        void sendBooksFlowable(Flowable<Book> books)
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchListenerSpec')
    @KafkaListener(batch = true, offsetReset = EARLIEST)
    @Topic(KafkaBatchListenerSpec.BOOKS_TOPIC)
    static class BookListener {
        List<Book> books = []
        List<String> headers = []
        List<String> keys = []

        @Topic(KafkaBatchListenerSpec.BOOKS_LIST_TOPIC)
        void receiveList(List<Book> books) {
            this.books.addAll books
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_LIST_CONSUMER_RECORDS_TOPIC)
        void receiveConsumerRecordList(List<ConsumerRecord<String, Book>> bookRecords) {
            this.books.addAll bookRecords.collect { it.value()}
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_LIST_WITH_KEYS_TOPIC)
        void receiveListWithKeys(@KafkaKey List<String> keys, List<Book> books) {
            this.books.addAll books
            this.keys.addAll keys
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_HEADERS_TOPIC)
        void receiveList(List<Book> books, @MessageHeader("X-Foo") List<String> foos) {
            this.books.addAll books
            this.headers = foos
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_ARRAY_TOPIC)
        void receiveArray(Book... books) {
            this.books.addAll Arrays.asList(books)
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_LIST_TOPIC)
        @SendTo(KafkaBatchListenerSpec.TITLES_TOPIC)
        List<String> receiveAndSendList(List<Book> books) {
            this.books.addAll(books)
            return books*.title
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_ARRAY_TOPIC)
        @SendTo(KafkaBatchListenerSpec.TITLES_TOPIC)
        String[] receiveAndSendArray(Book... books) {
            this.books.addAll Arrays.asList(books)
            return books*.title as String[]
        }

        @SendTo(KafkaBatchListenerSpec.TITLES_TOPIC)
        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_FLUX_TOPIC)
        Flux<String> receiveAndSendFlux(Flux<Book> books) {
            this.books.addAll books.collectList().block()
            return books.map { Book book -> book.title }
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_FLOWABLE_TOPIC)
        @SendTo(KafkaBatchListenerSpec.TITLES_TOPIC)
        Flowable<String> receiveAndSendFlowable(Flowable<Book> books) {
            this.books.addAll books.toList().blockingGet()
            return books.map { Book book -> book.title }
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_FLUX_TOPIC)
        void receiveFlux(Flux<Book> books) {
            this.books.addAll books.collectList().block()
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_FLOWABLE_TOPIC)
        void receiveFlowable(Flowable<Book> books) {
            this.books.addAll books.toList().blockingGet()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchListenerSpec')
    @KafkaListener(batch = true, offsetReset = EARLIEST)
    static class TitleListener {
        List<String> titles = []

        @Topic(KafkaBatchListenerSpec.TITLES_TOPIC)
        void receiveTitles(String... titles) {
            this.titles.addAll(titles)
        }
    }

    @ToString(includePackage = false)
    @EqualsAndHashCode
    static class Book {
        String title
    }
}
