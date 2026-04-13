package com.github.shangtanlin.mq.order;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.alibaba.fastjson.JSON;
import com.github.shangtanlin.config.mq.SubOrderMQConfig;
import com.github.shangtanlin.mapper.ParentOrderMapper;
import com.github.shangtanlin.mapper.SubOrderMapper;
import com.github.shangtanlin.mapper.mq.MqMessageLogMapper;
import com.github.shangtanlin.model.dto.es.OrderSyncMessage;
import com.github.shangtanlin.model.dto.es.SubOrderIndexDoc;
import com.github.shangtanlin.model.entity.mq.MqMessageLog;
import com.github.shangtanlin.model.entity.order.ParentOrder;
import com.github.shangtanlin.model.entity.order.SubOrder;
import com.github.shangtanlin.model.vo.order.SubOrderVO;
import com.github.shangtanlin.service.SubOrderService;
import com.rabbitmq.client.Channel;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Component
public class OrderSyncConsumer {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Resource
    private ElasticsearchClient elasticsearchClient;

    @Resource
    private SubOrderService subOrderService;

    @Autowired
    private ParentOrderMapper parentOrderMapper;

    @Autowired
    private SubOrderMapper subOrderMapper;

    @Autowired
    private MqMessageLogMapper mqMessageLogMapper;

    /**
     * 监听子订单 ES 同步队列（手动 ACK 模式）
     * concurrency = "3-5" 表示最少开启 3 个线程，最多 5 个线程并发处理
     */
    @RabbitListener(
            queues = SubOrderMQConfig.SUB_ORDER_ES_QUEUE,
            ackMode = "MANUAL",
            concurrency = "3-5"
    )
    public void onOrderSyncMessage(OrderSyncMessage syncMessage,
                                   Message message,
                                   Channel channel) throws IOException {

        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        // 1. 检查重试次数（从 x-death header 获取）
        long retryCount = getRetryCount(message);

        log.info("📝 [ES同步] 收到消息 | SubOrderId: {}, Type: {}, 重试次数: {}",
                syncMessage.getSubOrderId(), syncMessage.getType(), retryCount);

        // 2. 基础校验
        if (syncMessage == null || syncMessage.getSubOrderId() == null) {
            log.warn("📝 [ES同步] 收到无效消息，直接丢弃并签收");
            channel.basicAck(deliveryTag, false);
            return;
        }

        Long subOrderId = syncMessage.getSubOrderId();
        Integer type = syncMessage.getType();

        try {
            // 模拟异常
            SubOrder subOrder = null;
            subOrder.getId();

            // 3. 根据类型分流处理
            if (type == 1 || type == 2) {
                handleSaveOrUpdate(subOrderId);
            } else if (type == 3) {
                handleDelete(subOrderId);
            } else {
                log.warn("📝 [ES同步] 未知的操作类型: {}, SubOrderId: {}", type, subOrderId);
            }

            // 4. 业务成功，手动确认
            channel.basicAck(deliveryTag, false);
            log.info("📝 [ES同步] 消息处理成功并已签收 | SubOrderId: {}", subOrderId);

        } catch (Exception e) {
            log.error("📝 [ES同步] 业务报错 | SubOrderId: {}, 重试次数: {}, 错误: {}, 时间: {}",
                    subOrderId, retryCount, e.getMessage(), LocalDateTime.now().format(FORMATTER));

            // 5. 判断是否达到最大重试次数
            if (retryCount >= SubOrderMQConfig.MAX_RETRY_COUNT) {
                log.error("📝 [ES同步] 重试耗尽 | SubOrderId: {} 已达到最大重试次数 {}, 执行最终处理",
                        subOrderId, SubOrderMQConfig.MAX_RETRY_COUNT);
                handleFinalFailure(syncMessage, e);
                channel.basicAck(deliveryTag, false);  // 确认消息，不再重试
            } else {
                // 未达到最大重试次数，拒绝消息并进入死信队列
                log.warn("📝 [ES同步] 进入死信队列 | SubOrderId: {}, 等待 TTL 后重新消费",
                        subOrderId);
                channel.basicReject(deliveryTag, false);
            }
        }
    }

    /**
     * 从消息头获取重试次数（x-death header）
     */
    private long getRetryCount(Message message) {
        try {
            List<Map<String, ?>> xDeath = message.getMessageProperties().getXDeathHeader();

            if (xDeath != null && !xDeath.isEmpty()) {
                for (Map<String, ?> deathRecord : xDeath) {
                    String queue = (String) deathRecord.get("queue");
                    // 找到来自死信队列的记录（重试队列）
                    if (queue != null && queue.contains("dlx")) {
                        Object countObj = deathRecord.get("count");
                        return countObj != null ? Long.parseLong(countObj.toString()) : 0L;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("📝 [ES同步] 解析重试次数失败: {}", e.getMessage());
        }
        return 0L;
    }

    /**
     * ES 同步：新增或更新
     */
    private void handleSaveOrUpdate(Long subOrderId) throws IOException {
        // 1. 反查数据库：获取最全的订单数据
        SubOrderVO fullOrder = subOrderService.getDetailForEs(subOrderId);

        if (fullOrder == null) {
            log.warn("📝 [ES同步] 数据库中未找到子订单 {}, 可能已被删除", subOrderId);
            return;
        }

        // 2. 转换为 ES 文档对象
        SubOrderIndexDoc doc = convertToEsDoc(fullOrder);

        // 封装其他字段
        SubOrder subOrder = subOrderMapper.selectById(subOrderId);
        ParentOrder parentOrder = parentOrderMapper.selectById(subOrder.getParentOrderId());

        doc.setSubOrderId(subOrderId);
        doc.setParentOrderId(parentOrder.getId());
        doc.setParentOrderSn(parentOrder.getOrderSn());
        doc.setUserId(parentOrder.getUserId());

        // 3. 写入 ES（Index 操作天然幂等）
        IndexResponse response = elasticsearchClient.index(i -> i
                .index("sub_order_index")
                .id(subOrderId.toString())
                .document(doc)
        );

        log.info("📝 [ES同步] 成功 | SubOrderId: {}, ES响应: {}", subOrderId, response.result());
    }

    /**
     * ES 同步：删除
     */
    private void handleDelete(Long subOrderId) throws IOException {
        elasticsearchClient.delete(d -> d
                .index("sub_order_index")
                .id(subOrderId.toString())
        );
        log.info("📝 [ES同步] 删除成功 | SubOrderId: {}", subOrderId);
    }

    /**
     * 最终失败后的处理（落库）
     */
    private void handleFinalFailure(OrderSyncMessage msg, Exception e) {
        log.error("📝 [ES同步] 最终处理 | 记录失败消息到数据库: SubOrderId: {}, Error: {}",
                msg.getSubOrderId(), e.getMessage());

        try {
            String newId = UUID.randomUUID().toString();

            MqMessageLog messageLog = MqMessageLog.builder()
                    .id(newId)
                    .sourceType(1)  // 1-消费者端
                    .businessType(2)  // 2-子订单ES同步
                    .exchange(SubOrderMQConfig.SUB_ORDER_EXCHANGE)
                    .routingKey(SubOrderMQConfig.SUB_ORDER_SYNC_ROUTING_KEY)
                    .payload(JSON.toJSONString(msg))
                    .status(4)  // 4-人工处理
                    .retryCount(SubOrderMQConfig.MAX_RETRY_COUNT)
                    .cause("subOrderId=" + msg.getSubOrderId() + ", type=" + msg.getType() + ", error=" + e.getMessage())
                    .nextRetryTime(null)
                    .build();

            mqMessageLogMapper.insert(messageLog);
            log.info("📝 [ES同步] 失败消息已入库: 新ID={}, SubOrderId={}", newId, msg.getSubOrderId());

        } catch (Exception dbEx) {
            log.error("📝 [ES同步] 致命错误 | 失败消息入库失败! SubOrderId: {}, Error: {}",
                    msg.getSubOrderId(), dbEx.getMessage());
        }
    }

    /**
     * 数据转换：SubOrderVO → SubOrderIndexDoc
     */
    private SubOrderIndexDoc convertToEsDoc(SubOrderVO vo) {
        SubOrderIndexDoc doc = new SubOrderIndexDoc();

        // 金额字段转换（BigDecimal → Double）
        if (vo.getGoodsAmount() != null) {
            doc.setGoodsAmount(vo.getGoodsAmount().doubleValue());
        }
        if (vo.getPayAmount() != null) {
            doc.setPayAmount(vo.getPayAmount().doubleValue());
        }
        if (vo.getCouponAmount() != null) {
            doc.setCouponAmount(vo.getCouponAmount().doubleValue());
        }
        if (vo.getFreightAmount() != null) {
            doc.setFreightAmount(vo.getFreightAmount().doubleValue());
        }

        BeanUtils.copyProperties(vo, doc);

        // 处理嵌套的 items 列表
        if (vo.getItems() != null) {
            List<SubOrderIndexDoc.ItemInnerDTO> esItems = vo.getItems().stream().map(item -> {
                SubOrderIndexDoc.ItemInnerDTO esItem = new SubOrderIndexDoc.ItemInnerDTO();
                BeanUtils.copyProperties(item, esItem);
                if (item.getPrice() != null) {
                    esItem.setPrice(item.getPrice().doubleValue());
                }
                return esItem;
            }).collect(Collectors.toList());
            doc.setItems(esItems);
        }

        return doc;
    }
}