package com.github.shangtanlin.model.entity.mq;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * MQ消息记录表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MqMessageLog {

    /**
     * 消息唯一标识
     */
    private String id;

    /**
     * 来源类型: 0-生产者端, 1-消费者端
     */
    private Integer sourceType;

    /**
     * 业务类型: 0-购物车回流, 1-超时关单
     */
    private Integer businessType;

    /**
     * 交换机名称
     */
    private String exchange;

    /**
     * 路由键
     */
    private String routingKey;

    /**
     * 消息体内容 (JSON格式)
     */
    private String payload;

    /**
     * 状态:
     * 0-发送中
     * 1-成功
     * 2-路由失败
     * 3-投递失败
     * 4-人工处理
     */
    private Integer status;

    /**
     * 重试次数
     */
    private Integer retryCount;

    /**
     * 失败原因
     */
    private String cause;

    /**
     * 下次重试时间（生产者端用，消费者端为null）
     */
    private LocalDateTime nextRetryTime;

    /**
     * 创建时间
     */
    private LocalDateTime createTime;

    /**
     * 更新时间
     */
    private LocalDateTime updateTime;
}