/**
 * Copyright (C) 2017 Matthias Wessendorf.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.wessendorf.undertow.ws.kafka;

import io.undertow.Undertow;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.websockets.WebSocketConnectionCallback;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedBinaryMessage;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.CloseMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import io.undertow.websockets.spi.WebSocketHttpExchange;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

import static io.undertow.Handlers.path;
import static io.undertow.Handlers.resource;
import static io.undertow.Handlers.websocket;
import static net.wessendorf.utils.PropertiesResolverUtils.resolveKafkaService;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * Created by matzew on 5/8/17.
 */
public class WebSocketKafkaGateway {

    private static final Logger logger = Logger.getLogger(WebSocketKafkaGateway.class.getName());

    public static void main(String... args) {

        final Undertow server = Undertow.builder()
                .addHttpListener(8080, "0.0.0.0")
                .setHandler(path()
                        .addPrefixPath("/kafkabridge", websocket(new WebSocketConnectionCallback() {

                            @Override
                            public void onConnect(WebSocketHttpExchange exchange, WebSocketChannel channel) {

                                // configure and create produce

                                final Properties properties = new Properties();

                                properties.put(BOOTSTRAP_SERVERS_CONFIG, resolveKafkaService()+":9092");
                                properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                                properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

                                final Producer producer = new KafkaProducer<>(properties);



                                logger.info(String.format("Connection established from '%s'", channel.getPeerAddress()));

                                channel.getReceiveSetter().set(new AbstractReceiveListener() {

                                    @Override
                                    protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
                                        final String receivedTextPayload = message.getData();

                                        logger.info(String.format("Got text ('%s') from '%s'", receivedTextPayload, channel.getPeerAddress()));


                                        producer.send(new ProducerRecord("websocket_bridge", UUID.randomUUID().toString(), receivedTextPayload));
                                    }

                                    @Override
                                    protected void  onCloseMessage(CloseMessage cm, WebSocketChannel channel) {
                                        logger.info(String.format("Close request from '%s'", channel.getPeerAddress()));
                                    }

                                });
                                channel.resumeReceives();

                            }
                        }))
                        .addPrefixPath("/", resource(new ClassPathResourceManager(WebSocketKafkaGateway.class.getClassLoader(), WebSocketKafkaGateway.class.getPackage()))
                                .addWelcomeFiles("index.html")))
                .build();

        server.start();

        logger.warning("Server running.... connectiong to Kafka");
        createProducer().send(new ProducerRecord("websocket_bridge", UUID.randomUUID().toString(), "CONNECTED..........."));


        try {
            Thread.currentThread().join();
        }
        catch (InterruptedException e) {
            logger.info("shutting down");
            server.stop();
        }

    }

    private static Producer createProducer() {
        final Properties properties = new Properties();

        properties.put(BOOTSTRAP_SERVERS_CONFIG, resolveKafkaService()+":9092");
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final Producer producer = new KafkaProducer<>(properties);


        return producer;
    }

}
