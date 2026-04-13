package com.github.shangtanlin.config.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.shangtanlin.mapper.mq.MqMessageLogMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@Slf4j
public class RabbitMQBaseConfig {

    @Autowired
    private MqMessageLogMapper mqMessageLogMapper;

    /* ========== Message Converter ========== */
    @Bean
    @Primary
    public MessageConverter messageConverter() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    /**
     * RabbitTemplate 配置
     * 采用"先入库再发送"方案：
     * - 发送前：消息已入库（status=0-发送中）
     * - ReturnsCallback：更新 status=2（路由失败）
     * - ConfirmCallback ack=true：更新 status=1（成功）
     * - ConfirmCallback ack=false：更新 status=3（投递失败）
     */
    @Bean
    public RabbitTemplate rabbitTemplate(RabbitTemplateConfigurer configurer, ConnectionFactory connectionFactory,
                                         @Qualifier("messageConverter") MessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate();
        configurer.configure(template, connectionFactory);
        template.setMessageConverter(messageConverter);

        // ConfirmCallback：更新状态（不删除记录）
        template.setConfirmCallback((correlationData, ack, cause) -> {
            String msgId = correlationData != null ? correlationData.getId() : null;
            if (msgId == null) {
                log.warn("ConfirmCallback 收到无 ID 的确认，跳过处理");
                return;
            }

            if (ack) {
                // 消息到达 Exchange，尝试更新为成功
                // 仅当 status=0（发送中）时才更新，不会覆盖路由失败（status=2）
                int updated = mqMessageLogMapper.updateStatusToSuccess(msgId);
                if (updated > 0) {
                    log.info("消息投递成功: ID = {}", msgId);
                } else {
                    log.info("消息投递确认成功，但状态已被更新（如路由失败）: ID = {}", msgId);
                }
            } else {
                // 消息未到达 Exchange，更新为投递失败
                mqMessageLogMapper.updateStatusToSendFail(msgId, cause);
                log.error("消息投递失败: ID = {}, 原因 = {}", msgId, cause);
            }
        });

        // ReturnsCallback：路由失败，更新状态为 2
        template.setReturnsCallback(returned -> {
            String msgId = returned.getMessage().getMessageProperties().getCorrelationId();
            if (msgId == null) {
                log.warn("ReturnsCallback 收到无 ID 的退回消息，跳过处理");
                return;
            }

            String exchange = returned.getExchange();
            String routingKey = returned.getRoutingKey();
            String cause = "NO_ROUTE: " + returned.getReplyText();

            mqMessageLogMapper.updateStatusToRouteFail(msgId, cause);
            log.error("消息路由失败: ID = {}, 交换机 = {}, 路由键 = {}, 原因 = {}",
                    msgId, exchange, routingKey, cause);
        });

        // 测试连接
        try {
            connectionFactory.createConnection().close();
            log.info(">>>>>> RabbitMQ 连接成功！目标地址: {}:{} <<<<<<",
                    template.getConnectionFactory().getHost(),
                    template.getConnectionFactory().getPort());
        } catch (Exception e) {
            log.error(">>>>>> RabbitMQ 连接失败！请检查配置或服务状态 <<<<<<");
        }

        return template;
    }
}