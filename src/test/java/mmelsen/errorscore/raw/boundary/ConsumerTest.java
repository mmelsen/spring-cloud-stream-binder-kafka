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

import mmelsen.ErrorScore;
import mmelsen.LocalStore;
import mmelsen.SensorMeasurement;
import mmelsen.SensorMeasurementSerde;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class ConsumerTest {

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, "error-score");

    private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

    private static Consumer<String, ErrorScore> consumer;

    @Autowired
    private InteractiveQueryService interactiveQueryService;

    @BeforeClass
    public static void setUp() throws Exception {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<String, ErrorScore> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
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
        DefaultKafkaProducerFactory<String, SensorMeasurement> pf = new DefaultKafkaProducerFactory<>(senderProps, new StringSerializer(), new SensorMeasurementSerde().serializer());

//        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
//        DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
        try {
            KafkaTemplate<String, SensorMeasurement> template = new KafkaTemplate<>(pf, true);

            template.setDefaultTopic("error-score");
//            sendTestDataInWindows(template);

            sendSensorMeasurement(new SensorMeasurement(getDate(20, 22, 01), "tag", 0.1, 0.2, 0.1), template);
            sendSensorMeasurement(new SensorMeasurement(getDate(20, 23, 01), "tag", 0.2, 1.2, 0.2), template);
            sendSensorMeasurement(new SensorMeasurement(getDate(21, 02, 01), "tag", 0.3, 0.5, 0.3), template);
            sendSensorMeasurement(new SensorMeasurement(getDate(21, 10, 01), "tag", 0.4, 0.5, 0.4), template);
            sendSensorMeasurement(new SensorMeasurement(getDate(21, 22, 01), "tag", 0.5, 0.5, 0.5), template);
            sendSensorMeasurement(new SensorMeasurement(getDate(21, 23, 01), "tag", 0.6, 0.5, 0.6), template);
            sendSensorMeasurement(new SensorMeasurement(getDate(22, 23, 01), "tag", 0.6, 0.5, 0.7), template);
            sendSensorMeasurement(new SensorMeasurement(getDate(23, 23, 01), "tag", 0.6, 0.5, 0.7), template);
            ConsumerRecords<String, ErrorScore> cr = KafkaTestUtils.getRecords(consumer);
            assertThat(cr.count()).isGreaterThanOrEqualTo(1);

            Thread.sleep(2000);

            ReadOnlyWindowStore<String, ErrorScore> store = interactiveQueryService.getQueryableStore(LocalStore.ONE_DAY_STORE.getStoreName(),  QueryableStoreTypes.windowStore());
            KeyValueIterator<Windowed<String>, ErrorScore> storeIter = store.all();
            System.out.println("extra test ------------------------------------------");
            while(storeIter.hasNext()) {

                KeyValue<Windowed<String>, ErrorScore> next = storeIter.next();
                System.out.println(next.key.toString() + " : " + next.value.toString());
            }

//            ReadOnlyWindowStore<String, ErrorScore> localStore = interactiveQueryService.getQueryableStore("store",  QueryableStoreTypes.windowStore());

            ReadOnlyWindowStore<String, ErrorScore> oneHourStore = interactiveQueryService.getQueryableStore(LocalStore.ONE_HOUR_STORE.getStoreName(),  QueryableStoreTypes.windowStore());

            System.out.println("------------------------------------");
            System.out.println("One hour ---------------------------");
            System.out.println("------------------------------------");
            WindowStoreIterator<ErrorScore> oneHourIter = oneHourStore.fetch("null::tag", getDate(20, 21, 00).getTime(), getDate(29, 21, 00).getTime());

            while(oneHourIter.hasNext()) {
                KeyValue<Long, ErrorScore> next = oneHourIter.next();
                System.out.println(next.value.getTimestamp() + " " + next.value.getErrorSignal());
            }

            System.out.println("------------------------------------");
            System.out.println("Six hour ---------------------------");
            System.out.println("------------------------------------");
            ReadOnlyWindowStore<String, ErrorScore> sixHourStore = interactiveQueryService.getQueryableStore(LocalStore.SIX_HOUR_STORE.getStoreName(),  QueryableStoreTypes.windowStore());
            WindowStoreIterator<ErrorScore> sixHourIter = sixHourStore.fetch("null::tag", getDate(20, 21, 00).getTime(), getDate(29, 21, 00).getTime());

            int cnt = 0;
            while(sixHourIter.hasNext()) {
                KeyValue<Long, ErrorScore> next = sixHourIter.next();
                System.out.println(next.value.getTimestamp() + " " + next.value.getErrorSignal());
                cnt++;
            }
            assertThat(cnt == 3);

            System.out.println("------------------------------------");
            System.out.println("Twelve hour ------------------------");
            System.out.println("------------------------------------");
            ReadOnlyWindowStore<String, ErrorScore> twelveHourStore = interactiveQueryService.getQueryableStore(LocalStore.TWELVE_HOURS_STORE.getStoreName(),  QueryableStoreTypes.windowStore());
            WindowStoreIterator<ErrorScore> twelveHourIter = twelveHourStore.fetch("null::tag", getDate(20, 21, 00).getTime(), getDate(29, 21, 00).getTime());

            cnt = 0;
            while(twelveHourIter.hasNext()) {
                KeyValue<Long, ErrorScore> next = twelveHourIter.next();
                System.out.println(next.value.getTimestamp() + " " + next.value.getErrorSignal());
                cnt++;
            }
            assertThat(cnt = 2);

            System.out.println("------------------------------------");
            System.out.println("One day ----------------------------");
            System.out.println("------------------------------------");
            ReadOnlyWindowStore<String, ErrorScore> oneDayStore = interactiveQueryService.getQueryableStore(LocalStore.ONE_DAY_STORE.getStoreName(),  QueryableStoreTypes.windowStore());
            WindowStoreIterator<ErrorScore> oneDayIter = oneDayStore.fetch("null::tag", getDate(19, 21, 00).getTime(), getDate(29, 21, 00).getTime());

            while(oneDayIter.hasNext()) {
                KeyValue<Long, ErrorScore> next = oneDayIter.next();
                System.out.println(next.value.getTimestamp() + " " + next.value.getErrorSignal());
            }
        }
        finally {
            pf.destroy();
        }


    }

    public void sendSensorMeasurement(SensorMeasurement sm, KafkaTemplate template) {
        template.sendDefault(sm);

    }

    public Date getDate(int day, int hour, int minute) {

        LocalDateTime ldt = LocalDateTime.of(2019, Month.JANUARY, day, hour, minute, 40);
        ZonedDateTime zdt = ldt.atZone(ZoneId.systemDefault());
        return Date.from(zdt.toInstant());
    }

    public void sendTestDataInWindows(KafkaTemplate<String, SensorMeasurement> template) {
/*        template.sendDefault("test1");
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
        template.sendDefault("test5");*/
    }
}