/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mmelsen.errorscore.raw.boundary;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"server.port=0",
                "spring.jmx.enabled=false",
                "spring.cloud.stream.bindings.error-score-in.destination=error-score",
//                "spring.cloud.stream.bindings.output.destination=counts",
                "spring.cloud.stream.kafka.streams.default.consumer.application-id=basic-word-count",
                "spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
                "spring.cloud.stream.kafka.streams.binder.configuration.cache.max.bytes.buffering=0",
                "spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                "spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde"})
public class ConsumerTest {

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, "error-score");

    private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

    private static Consumer<String, String> consumer;

    @Autowired
    private InteractiveQueryService interactiveQueryService;

    @Autowired
    private InteractiveQueryService queryableStoreRegistry;

    @BeforeClass
    public static void setUp() throws Exception {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = cf.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "error-score");

        System.setProperty("spring.cloud.stream.kafka.streams.binder.brokers", embeddedKafka.getBrokersAsString());
    }

    @AfterClass
    public static void tearDown() {
        consumer.close();
        System.clearProperty("spring.cloud.stream.kafka.streams.binder.brokers");
    }

    @Test
    public void testKafkaStreamsWordCountProcessor() throws Exception {
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
        try {
            KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
            template.setDefaultTopic("error-score");
            template.sendDefault("test1");
            template.sendDefault("test11");
            template.sendDefault("test111");
            Thread.sleep(3000);
            template.sendDefault("test2");
            template.sendDefault("test22");
            template.sendDefault("test222");
            template.sendDefault("test2222");
            Thread.sleep(3000);
            template.sendDefault("test3");
            template.sendDefault("test33");
            template.sendDefault("test333");
            template.sendDefault("test3333");
            template.sendDefault("test33333");

            Thread.sleep(3000);
            template.sendDefault("test4");

            Thread.sleep(3000);
            template.sendDefault("test5");
            ConsumerRecords<String, String> cr = KafkaTestUtils.getRecords(consumer);
            assertThat(cr.count()).isGreaterThanOrEqualTo(1);

            Thread.sleep(2000);
            ReadOnlyWindowStore<String, String> localStore = queryableStoreRegistry.getQueryableStore("store",  QueryableStoreTypes.windowStore());
            KeyValueIterator<Windowed<String>, String> all = localStore.all();

            HashMap<Long, List<String>> result = new HashMap<>();
            while(all.hasNext()) {

                KeyValue<Windowed<String>, String> next = all.next();
                long startMs = next.key.window().start();
                long endMs = next.key.window().end();

                List<String> s = result.get(startMs);
                if(s == null) {
                    result.put(startMs, Arrays.asList(next.value));
                }
                else {
                    List<String> stringList = result.get(startMs);

                    List<String> temp = stringList.stream()
                            .collect(Collectors.toList());

                    temp.add(next.value);
                    result.put(startMs,temp);
                }
            }
            result.entrySet().stream().forEach(e-> {
                System.out.println(new Date(e.getKey()));
                result.get(e.getKey()).forEach(s -> System.out.println(s));
            });


            ReadOnlyWindowStore<String, String> store = queryableStoreRegistry.getQueryableStore("store",  QueryableStoreTypes.windowStore());
            KeyValueIterator<Windowed<String>, String> storeIter = store.all();
            System.out.println("extra test ------------------------------------------");
            while(storeIter.hasNext()) {

                KeyValue<Windowed<String>, String> next = storeIter.next();
                System.out.println(next.key + " : " + next.value);
            }

        }
        finally {
            pf.destroy();
        }
    }

}