package com.algaworks.algasensors.temperature.processing.infrastructure.rabbitmq;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class RabbitMQConfig {

    public static final String FANOUT_EXCHANGE_NAME = "temperature-processing.temperature-received.v1.e";
    public static final String DIRECT_EXCHANGE_NAME = "temperature-processing.notification.v1.e";

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

    @Bean
    public FanoutExchange fanoutExchange() {
        return ExchangeBuilder.fanoutExchange(FANOUT_EXCHANGE_NAME).build();
    }

    // --- Novo Fluxo de Notificação (Direct) ---
    @Bean
    public DirectExchange notificationExchange() {
        return ExchangeBuilder.directExchange(DIRECT_EXCHANGE_NAME).build();
    }
}
