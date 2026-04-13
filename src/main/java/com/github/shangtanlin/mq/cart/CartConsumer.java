package com.github.shangtanlin.mq.cart;

import com.alibaba.fastjson.JSON;
import com.github.shangtanlin.config.mq.CartMQConfig;
import com.github.shangtanlin.mapper.CartItemMapper;
import com.github.shangtanlin.mapper.SkuMapper;
import com.github.shangtanlin.mapper.mq.MqMessageLogMapper;
import com.github.shangtanlin.model.entity.cart.CartItem;
import com.github.shangtanlin.model.entity.mq.MqMessageLog;
import com.github.shangtanlin.model.entity.product.Sku;
import com.rabbitmq.client.Channel;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.github.shangtanlin.common.constant.CartConstant.*;
import static com.github.shangtanlin.common.constant.RedisConstant.CART_CACHE_KEY;
import static com.github.shangtanlin.config.mq.CartMQConfig.CART_QUEUE;

@Service
@Slf4j
public class CartConsumer {

    /**
     * 最大重试次数（从死信队列回流的最大次数）
     */
    private static final int MAX_RETRY_COUNT = 3;

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private MqMessageLogMapper mqMessageLogMapper;

    @Autowired
    private CartItemMapper cartItemMapper;

    @Autowired
    private SkuMapper skuMapper;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");


    @RabbitListener(queues = CART_QUEUE, ackMode = "MANUAL")
    public void handleCartWriteBack(CartWriteBackMessage cartWriteBackMessage,
                                    Message message,
                                    Channel channel) throws IOException {

        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        // 1. 检查重试次数（从 x-death header 获取）
        long retryCount = getRetryCount(message);

        log.info(">>>> [收到消息] ID: {}, 当前重试次数: {}",
                cartWriteBackMessage.getMsgId(), retryCount);

        try {
            // 模拟异常
            CartItem cartItem = null;
            cartItem.getId();

            // 2. 执行业务逻辑
            Integer type = cartWriteBackMessage.getType();
            if (WRITE_BACK_UPDATE.equals(type)) {
                handleUpdate(cartWriteBackMessage);
            } else if (WRITE_BACK_DELETE.equals(type)) {
                handleDelete(cartWriteBackMessage);
            } else if (WRITE_BACK_CLEAN.equals(type)) {
                handleClean(cartWriteBackMessage);
            }

            // 3. 业务成功，手动确认
            channel.basicAck(deliveryTag, false);
            log.info(">>>> 消息处理成功并已签收: {}", cartWriteBackMessage.getMsgId());

        } catch (Exception e) {
            log.error(">>>> [业务报错] 消息ID: {}, 重试次数: {}, 错误: {}, 时间: {}",
                    cartWriteBackMessage.getMsgId(), retryCount, e.getMessage(), LocalDateTime.now().format(FORMATTER));

            // 4. 判断是否达到最大重试次数
            if (retryCount >= MAX_RETRY_COUNT) {
                // 已达到最大重试次数，执行最终处理并确认消息
                log.error(">>>> [重试耗尽] 消息ID: {} 已达到最大重试次数 {}，执行最终处理",
                        cartWriteBackMessage.getMsgId(), MAX_RETRY_COUNT);
                handleFinalFailure(cartWriteBackMessage, e);
                channel.basicAck(deliveryTag, false);  // 确认消息，不再重试
            } else {
                // 未达到最大重试次数，拒绝消息并进入死信队列
                log.warn(">>>> [进入死信队列] 消息ID: {}, 等待 TTL 后重新消费",
                        cartWriteBackMessage.getMsgId());
                // requeue=false，消息进入死信队列，TTL 后自动回流到业务队列
                channel.basicReject(deliveryTag, false);
            }
        }
    }

    /**
     * 从消息头获取重试次数（x-death header）
     * x-death 是 RabbitMQ 自动添加的，记录消息进入死信队列的历史
     */
    private long getRetryCount(Message message) {
        try {
            List<Map<String, ?>> xDeath = message.getMessageProperties().getXDeathHeader();
            if (xDeath != null && !xDeath.isEmpty()) {
                Object countObj = xDeath.get(0).get("count");
                return countObj != null ? Long.parseLong(countObj.toString()) : 0L;
            }
        } catch (Exception e) {
            log.warn(">>>> [警告] 解析重试次数失败: {}", e.getMessage());
        }
        return 0L;
    }

    /**
     * 最终失败后的处理（落库）
     * 使用新的 ID 入库，避免与生产者记录主键冲突
     */
    private void handleFinalFailure(CartWriteBackMessage msg, Exception e) {
        log.error(">>>> [最终处理] 记录失败消息到数据库: MsgId: {}, Error: {}", msg.getMsgId(), e.getMessage());

        try {
            // 生成新的 ID，避免与生产者记录冲突
            String newId = UUID.randomUUID().toString();

            MqMessageLog messageLog = MqMessageLog.builder()
                    .id(newId)  // 使用新 ID
                    .sourceType(1)  // 1-消费者端
                    .businessType(0)  // 0-购物车回流
                    .exchange(CartMQConfig.CART_EXCHANGE)
                    .routingKey(CartMQConfig.CART_ROUTING_KEY)
                    .payload(JSON.toJSONString(msg))
                    .status(4)  // 4-人工处理（消费失败直接入库等待人工处理）
                    .retryCount(MAX_RETRY_COUNT)
                    .cause("originalMsgId=" + msg.getMsgId() + ", error=" + e.getMessage())  // 保留原始消息ID关联
                    .nextRetryTime(null)  // 消费者不参与定时任务重试
                    .build();

            mqMessageLogMapper.insert(messageLog);
            log.info(">>>> [最终处理] 失败消息已入库: 新ID={}, 原始MsgId={}", newId, msg.getMsgId());
        } catch (Exception dbEx) {
            log.error(">>>> [致命] 失败消息入库失败! MsgId: {}, Error: {}", msg.getMsgId(), dbEx.getMessage());
            // 入库失败也不要抛异常，避免消息无法确认导致无限循环
        }
    }

    /**
     * 更新数据
     */
    @Transactional(rollbackFor = Exception.class)
    public void handleUpdate(CartWriteBackMessage cartWriteBackMessage) {
        Long userId = cartWriteBackMessage.getUserId();
        Long skuId = cartWriteBackMessage.getSkuId();
        LocalDateTime msgTime = cartWriteBackMessage.getCreateTime();

        // 1. SKU 存在性校验
        Sku sku = skuMapper.selectById(skuId);
        if (sku == null) {
            log.warn("检测到非法 SKU: {}, 放弃入库并清理缓存", skuId);
            stringRedisTemplate.opsForHash().delete(CART_CACHE_KEY + userId, skuId.toString());
            return;
        }

        // 2. 查询现有记录
        CartItem existingItem = cartItemMapper.selectOne(userId, skuId);

        if (existingItem != null) {
            // 3. 时序校验：库中数据比消息新，直接丢弃
            if (!msgTime.isAfter(existingItem.getUpdateTime())) {
                log.info("检测到过期消息，忽略处理。SkuId: {}, 消息时间: {}, 库中时间: {}",
                        skuId, msgTime.format(TIME_FORMATTER), existingItem.getUpdateTime().format(TIME_FORMATTER));
                return;
            }

            // 4. 执行更新/恢复
            existingItem.setQuantity(cartWriteBackMessage.getCartRedisJson().getQuantity());
            existingItem.setChecked(cartWriteBackMessage.getCartRedisJson().getChecked());
            existingItem.setIsDelete(0);
            existingItem.setUpdateTime(msgTime);

            cartItemMapper.updateById(existingItem);
            log.debug("成功更新/恢复购物车记录: userId={}, skuId={}", userId, skuId);

        } else {
            // 5. 库中完全没记录，直接插入
            CartItem cartItem = new CartItem();
            cartItem.setUserId(userId);
            cartItem.setSkuId(skuId);
            cartItem.setQuantity(cartWriteBackMessage.getCartRedisJson().getQuantity());
            cartItem.setChecked(cartWriteBackMessage.getCartRedisJson().getChecked());
            cartItem.setIsDelete(0);
            cartItem.setUpdateTime(msgTime);

            cartItemMapper.insert(cartItem);
            log.debug("成功插入新购物车记录: userId={}, skuId={}", userId, skuId);
        }
    }

    /**
     * 删除数据
     */
    private void handleDelete(CartWriteBackMessage cartWriteBackMessage) {
        Long userId = cartWriteBackMessage.getUserId();
        Long skuId = cartWriteBackMessage.getSkuId();

        CartItem existing = cartItemMapper.selectOne(userId, skuId);

        if (existing == null) {
            log.info("删除购物车记录不存在，忽略处理。userId={}, skuId={}", userId, skuId);
            return;
        }

        // 时序校验：消息比库里数据新才执行删除
        if (!cartWriteBackMessage.getCreateTime().isAfter(existing.getUpdateTime())) {
            log.info("检测到过期删除消息，忽略处理。SkuId: {}, 消息时间: {}, 库中时间: {}",
                    skuId, cartWriteBackMessage.getCreateTime().format(TIME_FORMATTER), existing.getUpdateTime().format(TIME_FORMATTER));
            return;
        }

        // 执行逻辑删除
        existing.setIsDelete(1);
        existing.setUpdateTime(cartWriteBackMessage.getCreateTime());
        cartItemMapper.updateById(existing);
        log.info("成功删除购物车记录: userId={}, skuId={}", userId, skuId);
    }

    /**
     * 清空数据
     */
    private void handleClean(CartWriteBackMessage cartWriteBackMessage) {
        Long userId = cartWriteBackMessage.getUserId();
        LocalDateTime msgTime = cartWriteBackMessage.getCreateTime();

        int deletedCount = cartItemMapper.cleanAll(userId, msgTime);
        log.info("成功清空购物车记录: userId={}, 删除数量={}", userId, deletedCount);
    }

}