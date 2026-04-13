package com.github.shangtanlin.config.mq;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class SubOrderMQConfig {

    // ========== 业务队列常量 ==========
    public static final String SUB_ORDER_EXCHANGE = "order.sub.exchange";
    public static final String SUB_ORDER_ES_QUEUE = "order.sub.es.queue";
    public static final String SUB_ORDER_SYNC_ROUTING_KEY = "order.sub.sync";

    // ========== 死信队列常量（消费者重试）==========
    public static final String SUB_ORDER_DLX_EXCHANGE = "order.sub.dlx.exchange";
    public static final String SUB_ORDER_DLX_QUEUE = "order.sub.dlx.queue";
    public static final String SUB_ORDER_DLX_ROUTING_KEY = "order.sub.dlx.key";

    // ========== 重试间隔常量 ==========
    /** 消费者重试间隔：30秒 */
    public static final int RETRY_INTERVAL_MS = 30000;
    /** 最大重试次数 */
    public static final int MAX_RETRY_COUNT = 3;

    // ========== 业务队列配置 ==========

    /**
     * 业务交换机（TopicExchange）
     */
    @Bean
    public TopicExchange subOrderExchange() {
        return new TopicExchange(SUB_ORDER_EXCHANGE, true, false);
    }

    /**
     * 业务队列（配置死信交换机，消费者 reject 后进入死信队列）
     */
    @Bean
    public Queue subOrderEsQueue() {
        return QueueBuilder.durable(SUB_ORDER_ES_QUEUE)
                .deadLetterExchange(SUB_ORDER_DLX_EXCHANGE)
                .deadLetterRoutingKey(SUB_ORDER_DLX_ROUTING_KEY)
                .build();
    }

    /**
     * 业务绑定
     */
    @Bean
    public Binding subOrderEsBinding() {
        return BindingBuilder.bind(subOrderEsQueue())
                .to(subOrderExchange())
                .with(SUB_ORDER_SYNC_ROUTING_KEY);
    }

    // ========== 死信队列配置（消费者重试兜底）==========

    /**
     * 死信交换机
     */
    @Bean
    public DirectExchange subOrderDlxExchange() {
        return new DirectExchange(SUB_ORDER_DLX_EXCHANGE, true, false);
    }

    /**
     * 死信队列（TTL=30秒，过期后回流业务队列）
     */
    @Bean
    public Queue subOrderDlxQueue() {
        return QueueBuilder.durable(SUB_ORDER_DLX_QUEUE)
                .ttl(RETRY_INTERVAL_MS)
                .deadLetterExchange(SUB_ORDER_EXCHANGE)
                .deadLetterRoutingKey(SUB_ORDER_SYNC_ROUTING_KEY)
                .build();
    }

    /**
     * 死信绑定
     */
    @Bean
    public Binding subOrderDlxBinding() {
        return BindingBuilder.bind(subOrderDlxQueue())
                .to(subOrderDlxExchange())
                .with(SUB_ORDER_DLX_ROUTING_KEY);
    }
}