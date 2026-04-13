package com.github.shangtanlin.mq.order;

import com.alibaba.fastjson.JSON;
import com.github.shangtanlin.config.mq.OrderMQConfig;
import com.github.shangtanlin.mapper.mq.MqMessageLogMapper;
import com.github.shangtanlin.model.dto.order.OrderCancelMessage;
import com.github.shangtanlin.model.entity.mq.MqMessageLog;
import com.github.shangtanlin.model.entity.order.OrderItem;
import com.github.shangtanlin.service.OrderService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 超时订单消费者（手动 ack 模式 + 死信队列重试）
 * 监听 order.timeout.queue（消息从延迟队列过期后进入此队列）
 */
@Component
@Slf4j
public class OrderTimeoutListener {

    /**
     * 最大重试次数（从重试死信队列回流的最大次数）
     */
    private static final int MAX_RETRY_COUNT = 3;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Autowired
    private OrderService orderService;

    @Autowired
    private MqMessageLogMapper mqMessageLogMapper;

    /**
     * 处理超时关单消息（手动 ack 模式）
     * 监听业务队列（order.timeout.queue），不是延迟队列
     */
    @RabbitListener(queues = OrderMQConfig.TIMEOUT_QUEUE, ackMode = "MANUAL")
    public void onOrderTimeoutMessage(OrderCancelMessage cancelMessage,
                                      Message message,
                                      Channel channel) throws IOException {

        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        // 1. 检查重试次数（从 x-death header 获取）
        long retryCount = getRetryCount(message);

        log.info("⏰ [超时关单] 收到消息 | OrderSn: {}, UserId: {}, 当前重试次数: {}",
                cancelMessage.getOrderSn(), cancelMessage.getUserId(), retryCount);

        // 2. 基础判空校验（防御式编程）
        if (cancelMessage == null || cancelMessage.getOrderSn() == null || cancelMessage.getUserId() == null) {
            log.warn("⏰ [超时关单] 收到无效消息，直接丢弃并签收: {}", cancelMessage);
            channel.basicAck(deliveryTag, false);
            return;
        }

        String orderSn = cancelMessage.getOrderSn();
        Long userId = cancelMessage.getUserId();

        try {
            // 模拟异常
            //OrderItem orderItem = null;
            //orderItem.getId();

            // 3. 调用 Service 执行核心关单逻辑
            // 该方法内部应包含：状态校验、修改状态、回滚库存、回滚优惠券
            boolean isClosed = orderService.cancelOrder(orderSn, userId);

            // 4. 根据处理结果记录日志
            if (isClosed) {
                log.info("✅ [超时关单] 订单 {} 处理成功，已关闭并释放资源", orderSn);
            } else {
                // 说明订单可能已经支付了，或者已经被手动取消了，这属于正常业务跳过
                log.info("ℹ️ [超时关单] 订单 {} 无需处理（可能已支付或已取消）", orderSn);
            }

            // 5. 业务成功，手动确认
            channel.basicAck(deliveryTag, false);
            log.info("⏰ [超时关单] 消息处理成功并已签收: OrderSn: {}", orderSn);

        } catch (Exception e) {
            log.error("⏰ [超时关单] 业务报错 | OrderSn: {}, 重试次数: {}, 错误: {}, 时间: {}",
                    orderSn, retryCount, e.getMessage(), LocalDateTime.now().format(FORMATTER));

            // 6. 判断是否达到最大重试次数
            if (retryCount >= MAX_RETRY_COUNT) {
                // 已达到最大重试次数，执行最终处理并确认消息
                log.error("⏰ [超时关单] 重试耗尽 | OrderSn: {} 已达到最大重试次数 {}，执行最终处理",
                        orderSn, MAX_RETRY_COUNT);
                handleFinalFailure(cancelMessage, e);
                channel.basicAck(deliveryTag, false);  // 确认消息，不再重试
            } else {
                // 未达到最大重试次数，拒绝消息并进入死信队列
                log.warn("⏰ [超时关单] 进入死信队列 | OrderSn: {}, 等待 TTL 后重新消费",
                        orderSn);
                // requeue=false，消息进入死信队列，TTL 后自动回流到业务队列
                channel.basicReject(deliveryTag, false);
            }
        }
    }

    /**
     * 从消息头获取重试次数（x-death header）
     * x-death 是 RabbitMQ 自动添加的，记录消息进入死信队列的历史
     *
     * x-death 是一个数组，每条记录包含：queue, reason, count, time, routing-keys 等
     * 我们需要找到来自 retry.dlx.queue 的记录，获取其 count 作为消费重试次数
     */
    private long getRetryCount(Message message) {
        try {
            List<Map<String, ?>> xDeath = message.getMessageProperties().getXDeathHeader();

            // 打印完整的 x-death header 用于调试
            log.debug("x-death header: {}", xDeath);

            if (xDeath != null && !xDeath.isEmpty()) {
                // 遍历所有死信记录，找到来自重试死信队列的记录
                for (Map<String, ?> deathRecord : xDeath) {
                    String queue = (String) deathRecord.get("queue");
                    log.debug("死信记录: queue={}, count={}", queue, deathRecord.get("count"));

                    // 找到来自 retry.dlx.queue 的记录，这才是消费重试
                    if (queue != null && queue.contains("retry")) {
                        Object countObj = deathRecord.get("count");
                        return countObj != null ? Long.parseLong(countObj.toString()) : 0L;
                    }
                }

                // 如果没有找到 retry 相关记录，说明是正常的 TTL 过期流程（首次消费）
                // 返回 0 表示没有消费重试
                return 0L;
            }
        } catch (Exception e) {
            log.warn("⏰ [超时关单] 解析重试次数失败: {}", e.getMessage());
        }
        return 0L;
    }

    /**
     * 最终失败后的处理（落库）
     * 使用新的 ID 入库，避免与生产者记录主键冲突
     */
    private void handleFinalFailure(OrderCancelMessage msg, Exception e) {
        log.error("⏰ [超时关单] 最终处理 | 记录失败消息到数据库: OrderSn: {}, Error: {}", msg.getOrderSn(), e.getMessage());

        try {
            // 生成新的 ID，避免与生产者记录冲突
            String newId = UUID.randomUUID().toString();

            MqMessageLog messageLog = MqMessageLog.builder()
                    .id(newId)  // 使用新 ID
                    .sourceType(1)  // 1-消费者端
                    .businessType(1)  // 1-超时关单
                    .exchange(OrderMQConfig.DLX_EXCHANGE)
                    .routingKey(OrderMQConfig.TIMEOUT_ROUTING_KEY)
                    .payload(JSON.toJSONString(msg))
                    .status(4)  // 4-人工处理（消费失败直接入库等待人工处理）
                    .retryCount(MAX_RETRY_COUNT)
                    .cause("orderSn=" + msg.getOrderSn() + ", userId=" + msg.getUserId() + ", error=" + e.getMessage())  // 保留原始订单信息
                    .nextRetryTime(null)  // 消费者不参与定时任务重试
                    .build();

            mqMessageLogMapper.insert(messageLog);
            log.info("⏰ [超时关单] 失败消息已入库: 新ID={}, 原OrderSn={}", newId, msg.getOrderSn());
        } catch (Exception dbEx) {
            log.error("⏰ [超时关单] 致命错误 | 失败消息入库失败! OrderSn: {}, Error: {}", msg.getOrderSn(), dbEx.getMessage());
            // 入库失败也不要抛异常，避免消息无法确认导致无限循环
        }
    }
}