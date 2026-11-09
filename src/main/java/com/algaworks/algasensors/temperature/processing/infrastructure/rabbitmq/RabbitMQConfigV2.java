package com.algaworks.algasensors.temperature.processing.infrastructure.rabbitmq;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class RabbitMQConfigV2 {

    @Bean
    public JacksonJsonMessageConverter jacksonJsonMessageConverter(
            JsonMapper jsonMapper
    ) {
        return new JacksonJsonMessageConverter(jsonMapper);
    }

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    // --- 1. CONFIGURAÇÃO DA FANOUT (Auditoria e Dashboard) ---
    public static final String FANOUT_EXCHANGE = "sensor.temperature.fanout";

    @Bean
    public FanoutExchange fanoutExchange() {
        return new FanoutExchange(FANOUT_EXCHANGE);
    }

    // --- 2. CONFIGURAÇÃO DA DIRECT (Manutenção Baseada em Severidade) ---
    public static final String DIRECT_EXCHANGE = "sensor.temperature.direct";
    // public static final String QUEUE_MAINTENANCE = "sensor.maintenance.queue";

    @Bean
    public DirectExchange directExchange() {
        return new DirectExchange(DIRECT_EXCHANGE);
    }

    // --- 3. CONFIGURAÇÃO DA TOPIC (Filtros Regionais / Hierárquicos) ---
    public static final String TOPIC_EXCHANGE = "sensor.temperature.topic";

    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange(TOPIC_EXCHANGE);
    }

    // --- 4. CONFIGURAÇÃO DA HEADERS (Roteamento por Metadados no Cabeçalho) ---
    public static final String HEADERS_EXCHANGE = "sensor.temperature.headers";

    @Bean
    public HeadersExchange headersExchange() {
        return new HeadersExchange(HEADERS_EXCHANGE);
    }

}
