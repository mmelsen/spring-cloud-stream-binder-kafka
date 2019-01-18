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

import mmelsen.errorscore.raw.entity.SensorMeasurement;
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
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

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
                "spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                "spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde=mmelsen.errorscore.raw.entity.ErrorScoreSerde"})
public class ConsumerTest {

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, "error-score");

    private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

    private static Consumer<String, String> consumer;

    @Autowired
    private QueryableStoreRegistry queryableStoreRegistry;

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
        DefaultKafkaProducerFactory<String, SensorMeasurement> pf = new DefaultKafkaProducerFactory<>(senderProps);
        try {
            KafkaTemplate<String, SensorMeasurement> template = new KafkaTemplate<>(pf, true);
            template.setDefaultTopic("error-score");
            template.sendDefault(new SensorMeasurement( Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()), "test", 0.1, 0.3, 0.2));

            Thread.sleep(2000);
            template.sendDefault(new SensorMeasurement( Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()), "test", 0.3, 0.5, 0.2));

            Thread.sleep(2000);
            template.sendDefault(new SensorMeasurement( Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()), "test", 0.5, 0.7, 0.2));

            ConsumerRecords<String, String> cr = KafkaTestUtils.getRecords(consumer);
            assertThat(cr.count()).isGreaterThanOrEqualTo(1);


            // IDEALLY I WANT TO VERIFY THE CONTENTS OF THE STORE
            ReadOnlyWindowStore<String, String> localStore = queryableStoreRegistry.getQueryableStoreType("store",  QueryableStoreTypes.windowStore());
            KeyValueIterator<Windowed<String>, String> all = localStore.all();

            while(all.hasNext()) {

                KeyValue<Windowed<String>, String> next = all.next();
                System.out.println(next.key + " : " + next.value);
            }
        }
        finally {
            pf.destroy();
        }
    }

}