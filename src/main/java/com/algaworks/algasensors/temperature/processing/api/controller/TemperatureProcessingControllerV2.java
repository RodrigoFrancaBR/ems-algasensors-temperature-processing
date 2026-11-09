package com.algaworks.algasensors.temperature.processing.api.controller;

import com.algaworks.algasensors.temperature.processing.api.model.TemperatureLogOutput;
import com.algaworks.algasensors.temperature.processing.common.IdGenerator;
import com.algaworks.algasensors.temperature.processing.infrastructure.rabbitmq.RabbitMQConfigV2;
import io.hypersistence.tsid.TSID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.OffsetDateTime;
import java.util.Random;

@RestController
@RequestMapping("/api/sensors/{sensorId}/temperatures/data")
@Slf4j
@RequiredArgsConstructor
public class TemperatureProcessingControllerV2 {

    private final RabbitTemplate rabbitTemplate;

    @PostMapping(consumes = MediaType.TEXT_PLAIN_VALUE)
    public void data(@PathVariable TSID sensorId, @RequestBody String input) {
        if (input == null || input.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }

        Double temperature;

        try {
            temperature = Double.parseDouble(input);
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }

        TemperatureLogOutput logOutput = getTemperatureLogOutput(sensorId, temperature);

        log.info("Processando leitura do sensor {}: {}°C", logOutput.getSensorId(), logOutput.getValue());

        sendtoGeneralBroadcast(logOutput);
        sendToMaintenance(logOutput);
        sendToGlobalAndRegionalAnalyses(logOutput);
    }

    // --- NOVO ENDPOINT DE GERAÇÃO EM MASSA (N sensores e N temperaturas) ---
    @PostMapping("/generate/{count}")
    public ResponseEntity<String> generateBulkTemperatures(@PathVariable int count) {
        if (count <= 0 || count > 10000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "A quantidade deve ser entre 1 e 10000.");
        }

        Random random = new Random();

        for (int i = 0; i < count; i++) {
            TSID randomSensorId = getTsid(i);

            double randomTemperature = getRandomTemperature(random);

            TemperatureLogOutput logOutput = getTemperatureLogOutput(randomSensorId, randomTemperature);

            log.info("Gerando massa [{}/{}] - Sensor {}: {}°C", (i + 1), count, logOutput.getSensorId(), logOutput.getValue());

            // 4. Dispara para as 4 exchanges
            sendtoGeneralBroadcast(logOutput);
            sendToMaintenance(logOutput);
            sendToGlobalAndRegionalAnalyses(logOutput);
            sendToHeadersExchange(logOutput);
        }

        return ResponseEntity.ok(String.format("Sucesso! %d leituras geradas e publicadas no RabbitMQ.", count));
    }

    private static TemperatureLogOutput getTemperatureLogOutput(TSID randomSensorId, double randomTemperature) {
        // 3. Monta o objeto de saída padronizado
        TemperatureLogOutput logOutput = TemperatureLogOutput.builder()
                .id(IdGenerator.generateTimeBasedUUID())
                .sensorId(randomSensorId)
                .value(randomTemperature)
                .registeredAt(OffsetDateTime.now())
                .build();
        return logOutput;
    }

    private static TSID getTsid(int i) {
        // 1. Gera um ID de sensor simulado (usando TSID ou gerador compatível)
        TSID randomSensorId = TSID.from(System.currentTimeMillis() + i);
        return randomSensorId;
    }

    private static double getRandomTemperature(Random random) {
        // 2. Gera uma temperatura aleatória entre 15.0°C e 95.0°C (testando tanto normais quanto críticas)
        double randomTemperature = 15.0 + (95.0 - 15.0) * random.nextDouble();
        // Arredonda para 2 casas decimais
        randomTemperature = Math.round(randomTemperature * 100.0) / 100.0;
        return randomTemperature;
    }

    private void sendtoGeneralBroadcast(TemperatureLogOutput logOutput) {
        // --- 1. FANOUT: Envia para o Broadcast Geral (Auditoria e Dashboard) ---
        // A routing key é ignorada (string vazia ""), vai para todas as filas ligadas à fanout.
        log.info("[1/3 FANOUT] Enviando cópia para Auditoria e Dashboard em tempo real.");
        rabbitTemplate.convertAndSend(RabbitMQConfigV2.FANOUT_EXCHANGE, "", logOutput);
    }

    private void sendToMaintenance(TemperatureLogOutput logOutput) {
        // --- 2. DIRECT: Envia para Manutenção se o valor for crítico ---
        String directRoutingKey = logOutput.getValue() > 80.0 ? "alerta.critico" : "alerta.normal";
        log.info("[2/3 DIRECT] Direcionando para manutenção com chave: '{}'", directRoutingKey);
        rabbitTemplate.convertAndSend(RabbitMQConfigV2.DIRECT_EXCHANGE, directRoutingKey, logOutput);
    }

    private void sendToGlobalAndRegionalAnalyses(TemperatureLogOutput logOutput) {
        // --- 3. TOPIC: Envia com chave hierárquica para análises globais/regionais ---
        // Exemplo simulado: país "brasil", setor "caldeiraria", status baseado na temperatura
        String status = logOutput.getValue() > 80.0 ? "critico" : "estavel";
        String topicRoutingKey = String.format("brasil.caldeiraria.%s", status);

        log.info("[3/3 TOPIC] Publicando com chave hierárquica: '{}'", topicRoutingKey);
        rabbitTemplate.convertAndSend(RabbitMQConfigV2.TOPIC_EXCHANGE, topicRoutingKey, logOutput);
    }

    private void sendToHeadersExchange(TemperatureLogOutput logOutput) {
        // --- 4. HEADERS: Roteamento baseado em atributos de cabeçalho ---
        log.info("[4/4 HEADERS] Enviando mensagem com headers personalizados.");

        // Define a severidade baseada na regra de negócio
        String severidade = logOutput.getValue() > 80.0 ? "critico" : "normal";

        rabbitTemplate.convertAndSend(
                RabbitMQConfigV2.HEADERS_EXCHANGE,
                "", // Na Headers Exchange, a routing key é ignorada
                logOutput,
                message -> {
                    // Inserindo os pares de chave-valor no cabeçalho da mensagem
                    message.getMessageProperties().setHeader("formato", "json");
                    message.getMessageProperties().setHeader("severidade", severidade);
                    return message;
                }
        );
    }
}
