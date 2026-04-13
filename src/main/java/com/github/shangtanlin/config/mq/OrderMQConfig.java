package com.github.shangtanlin.config.mq;

import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 超时关单 MQ 配置
 * 使用 TTL + 死信队列 实现延迟消息
 *
 * 架构：
 * 消息 → delay.exchange → delay.queue(TTL=15分钟) → dlx.exchange → timeout.queue → 消费者
 */
@Configuration
public class OrderMQConfig {

    // ========== 延迟队列配置（TTL 队列，无消费者）==========
    public static final String DELAY_EXCHANGE = "order.delay.exchange";
    public static final String DELAY_QUEUE = "order.delay.queue";  // TTL 队列
    public static final String DELAY_ROUTING_KEY = "order.delay.key";

    // ========== 死信交换机 + 业务队列 ==========
    public static final String DLX_EXCHANGE = "order.dlx.exchange";
    public static final String TIMEOUT_QUEUE = "order.timeout.queue";  // 实际消费的队列
    public static final String TIMEOUT_ROUTING_KEY = "order.timeout.key";

    // ========== 消费者重试死信队列（消费失败后重试）==========
    public static final String RETRY_DLX_EXCHANGE = "order.retry.dlx.exchange";
    public static final String RETRY_DLX_QUEUE = "order.retry.dlx.queue";
    public static final String RETRY_DLX_ROUTING_KEY = "order.retry.dlx.key";

    // ========== 延迟时间常量 ==========
    /** 订单超时时间：15分钟 = 15 * 60 * 1000 ms */
    public static final int ORDER_TIMEOUT_MS = 15 * 60 * 1000;
    //public static final int ORDER_TIMEOUT_MS =  10 * 1000;
    /** 消费者重试间隔：30秒 */
    public static final int RETRY_INTERVAL_MS = 30000;
    //public static final int RETRY_INTERVAL_MS = 3000;

    // ========== 延迟队列配置（消息先到这里，等待 TTL 到期）==========

    /**
     * 延迟交换机（普通 Direct 类型，支持 mandatory）
     */
    @Bean
    public DirectExchange delayExchange() {
        return new DirectExchange(DELAY_EXCHANGE, true, false);
    }

    /**
     * 延迟队列（设置 TTL + 死信交换机）
     * 注意：这个队列不绑定消费者，只用于暂存消息等待过期
     */
    @Bean
    public Queue delayQueue() {
        return QueueBuilder.durable(DELAY_QUEUE)
                .ttl(ORDER_TIMEOUT_MS)  // 15分钟 TTL
                .deadLetterExchange(DLX_EXCHANGE)  // 过期后进入死信交换机
                .deadLetterRoutingKey(TIMEOUT_ROUTING_KEY)
                .build();
    }

    /**
     * 延迟绑定
     */
    @Bean
    public Binding delayBinding(
            @Qualifier("delayQueue") Queue delayQueue,
            @Qualifier("delayExchange") DirectExchange delayExchange) {
        return BindingBuilder.bind(delayQueue)
                .to(delayExchange)
                .with(DELAY_ROUTING_KEY);
    }

    // ========== 死信交换机 + 业务队列（消息过期后进入这里被消费）==========

    /**
     * 死信交换机（接收延迟队列过期后的消息）
     */
    @Bean
    public DirectExchange dlxExchange() {
        return new DirectExchange(DLX_EXCHANGE, true, false);
    }

    /**
     * 业务队列（消费者监听这个队列）
     * 配置消费者重试死信队列，消费失败后进入重试队列
     */
    @Bean
    public Queue timeoutQueue() {
        return QueueBuilder.durable(TIMEOUT_QUEUE)
                .deadLetterExchange(RETRY_DLX_EXCHANGE)  // 消费失败后进入重试死信交换机
                .deadLetterRoutingKey(RETRY_DLX_ROUTING_KEY)
                .build();
    }

    /**
     * 死信绑定（延迟队列过期后的消息路由到业务队列）
     */
    @Bean
    public Binding dlxBinding(
            @Qualifier("timeoutQueue") Queue timeoutQueue,
            @Qualifier("dlxExchange") DirectExchange dlxExchange) {
        return BindingBuilder.bind(timeoutQueue)
                .to(dlxExchange())
                .with(TIMEOUT_ROUTING_KEY);
    }

    // ========== 消费者重试死信队列配置 ==========

    /**
     * 重试死信交换机
     */
    @Bean
    public DirectExchange retryDlxExchange() {
        return new DirectExchange(RETRY_DLX_EXCHANGE, true, false);
    }

    /**
     * 重试死信队列（TTL=30秒，过期后回流业务队列）
     */
    @Bean
    public Queue retryDlxQueue() {
        return QueueBuilder.durable(RETRY_DLX_QUEUE)
                .ttl(RETRY_INTERVAL_MS)  // 30秒后回流
                .deadLetterExchange(DLX_EXCHANGE)  // 回流到死信交换机（最终到业务队列）
                .deadLetterRoutingKey(TIMEOUT_ROUTING_KEY)
                .build();
    }

    /**
     * 重试死信绑定
     */
    @Bean
    public Binding retryDlxBinding(
            @Qualifier("retryDlxQueue") Queue retryDlxQueue,
            @Qualifier("retryDlxExchange") DirectExchange retryDlxExchange) {
        return BindingBuilder.bind(retryDlxQueue)
                .to(retryDlxExchange())
                .with(RETRY_DLX_ROUTING_KEY);
    }
}