package com.github.shangtanlin.service.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import co.elastic.clients.util.NamedValue;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;

import com.baomidou.mybatisplus.core.toolkit.IdWorker;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.github.shangtanlin.common.constant.OrderStatusConstant;
import com.github.shangtanlin.common.constant.RedisConstant;
import com.github.shangtanlin.common.exception.BusinessException;
import com.github.shangtanlin.common.utils.OrderStatusUtil;
import com.github.shangtanlin.common.utils.UserHolder;
import com.github.shangtanlin.config.ElasticsearchConfig;
import com.github.shangtanlin.config.mq.OrderMQConfig;
import com.github.shangtanlin.config.mq.SubOrderMQConfig;
import com.github.shangtanlin.mapper.*;

import com.github.shangtanlin.mapper.coupon.CouponTemplateMapper;
import com.github.shangtanlin.mapper.coupon.CouponUserRecordMapper;
import com.github.shangtanlin.mapper.mq.MqMessageLogMapper;
import cn.hutool.json.JSONUtil;
import com.github.shangtanlin.model.dto.es.OrderSyncMessage;
import com.github.shangtanlin.model.dto.es.SubOrderIndexDoc;
import com.github.shangtanlin.model.dto.order.OrderCancelMessage;
import com.github.shangtanlin.model.dto.order.OrderConfirmDTO;
import com.github.shangtanlin.model.dto.order.OrderItemDTO;
import com.github.shangtanlin.model.dto.order.OrderSubmitDTO;
import com.github.shangtanlin.model.dto.mq.MqCorrelationData;
import com.github.shangtanlin.model.entity.cart.CartItem;
import com.github.shangtanlin.model.entity.mq.MqMessageLog;
import com.github.shangtanlin.model.entity.order.OrderItem;
import com.github.shangtanlin.model.entity.order.ParentOrder;
import com.github.shangtanlin.model.entity.order.SubOrder;
import com.github.shangtanlin.model.entity.product.Sku;
import com.github.shangtanlin.model.entity.product.Spu;
import com.github.shangtanlin.model.entity.shop.Shop;
import com.github.shangtanlin.model.entity.user.UserAddress;
import com.github.shangtanlin.model.enums.OrderStatusEnum;
import com.github.shangtanlin.model.enums.PaymentTypeEnum;
import com.github.shangtanlin.model.vo.OrderCreateVO;
import com.github.shangtanlin.model.vo.coupon.UserCouponVO;
import com.github.shangtanlin.model.vo.order.OrderConfirmVO;
import com.github.shangtanlin.model.vo.order.SubOrderVO;
import com.github.shangtanlin.result.PageResult;
import com.github.shangtanlin.service.*;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@Slf4j
public class OrderServiceImpl
        extends ServiceImpl<ParentOrderMapper, ParentOrder> // 1. 继承基类，获得 CRUD 能力
        implements OrderService {

    @Autowired
    private ElasticsearchClient elasticsearchClient;


    @Autowired
    private StringRedisTemplate stringRedisTemplate;


    @Autowired
    private SkuService skuService;



    @Autowired
    private UserAddressMapper userAddressMapper;


    @Autowired
    private CouponUserRecordMapper couponUserRecordMapper;


    @Autowired
    private CouponTemplateMapper couponTemplateMapper;


    @Autowired
    private CouponService couponService;

    @Autowired
    private ShopMapper shopMapper;

    @Autowired
    private SkuMapper skuMapper;

    @Autowired
    private SpuMapper spuMapper;


    @Autowired
    private ParentOrderMapper parentOrderMapper;

    @Autowired
    @Lazy
    private SubOrderService subOrderService;

    @Autowired
    private CartItemMapper cartItemMapper;

    @Autowired
    private OrderItemService orderItemService;

    @Autowired
    private OrderItemMapper orderItemMapper;

    @Autowired
    private SubOrderMapper subOrderMapper;


    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private MqMessageLogMapper mqMessageLogMapper;




    /**
     * 更新主订单状态
     * @param parentOrderSn 父订单编号
     * @param status 目标状态码 (来自 OrderStatusEnum)
     */
    @Override
    @Transactional(rollbackFor = Exception.class) // 建议加上事务，保证操作安全
    public void updateStatus(String parentOrderSn, Integer status) {
        // 1. 获取当前登录用户 ID (安全隔离，防止越权修改他人订单)
        Long userId = UserHolder.getUser().getId();

        // 2. 构造更新条件
        // SQL 逻辑：UPDATE parent_order SET status = ? WHERE parent_order_sn = ? AND user_id = ?
        boolean success = this.update(new LambdaUpdateWrapper<ParentOrder>()
                .eq(ParentOrder::getOrderSn, parentOrderSn)
                .eq(ParentOrder::getUserId, userId)
                .set(ParentOrder::getStatus, status)
                .set(ParentOrder::getUpdateTime, LocalDateTime.now())); // 顺便更新下修改时间

        // 3. 校验执行结果
        if (!success) {
            log.error("更新主订单状态失败，订单编号可能不存在或归属错误: {}, 用户ID: {}", parentOrderSn, userId);
            throw new BusinessException("更新主订单状态失败");
        }

        log.info("主订单 {} 状态已成功更新为 {}", parentOrderSn, status);
    }




    /**
     * 预下单
     * @param dto
     * @return
     */
    @Override
    public OrderConfirmVO getPreOrder(OrderConfirmDTO dto) {
        // 0. 生成并封装防重token
        // 2. 获取当前登录用户 ID
        Long userId = UserHolder.getUser().getId();

        // 3. 生成防重提交 Token (UUID)
        String token = UUID.randomUUID().toString().replace("-", "");

        // 4. 存入 Redis，设置 30 分钟有效期
        // Key 结构：order:submit:token:用户ID:UUID
        String redisKey = RedisConstant.ORDER_SUBMIT_KEY + userId + ":" + token;
        stringRedisTemplate.opsForValue().set(redisKey, "1", 30, TimeUnit.MINUTES);

        OrderConfirmVO vo = new OrderConfirmVO();

        // 1. 初始化总金额变量
        BigDecimal goodsAmount = BigDecimal.ZERO;   //商品总价
        BigDecimal totalFreight = BigDecimal.ZERO;  //商品总运费
        BigDecimal couponAmount = BigDecimal.ZERO;

        // 1. 将 ShopOrderRequest 列表转换为 ConfirmSubOrderVO 列表
        //subOrders是返回给前端的每一个店铺对应的所有商品信息，包含了店铺的id，logo，以及店铺下的所有商品
        //包括该店铺的运费，分摊到的商品优惠，该店铺的商品总价
        //dto的shops里包含了该店铺的id，该店铺选中的优惠券，该店铺的商品信息，商品信息里又包含skuId和quantity
        List<OrderConfirmVO.ConfirmSubOrderVO> subOrders = dto.getShops().stream()
                .map(this::processShopOrder) // 转换每个店铺的数据
                .collect(Collectors.toList());

        vo.setSubOrders(subOrders);


        // 2. 从转换后的列表中统计总额(总价和总运费已经算完了)
        // 使用 reduce 累加 BigDecimal
        goodsAmount = subOrders.stream()
                .map(OrderConfirmVO.ConfirmSubOrderVO::getSubTotalAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        totalFreight = subOrders.stream()
                .map(OrderConfirmVO.ConfirmSubOrderVO::getSubFreightAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        //封装4个金额
        vo.setGoodsAmount(goodsAmount);
        vo.setFreightAmount(totalFreight);

        //计算每个店铺分摊到的优惠，以及每个店铺的实付金额
        // 1. 如果没有传入优惠券ID，查询并自动匹配最优券
        if (dto.getCouponUserRecordId() == null) {
            List<UserCouponVO> availableCoupons = couponService.getAvailableCoupons(goodsAmount);

            // 2. 使用 Stream 流寻找：可用(available=true) 且 减免金额(reduceAmount) 最大的一张
            UserCouponVO bestCoupon = availableCoupons.stream()
                    .filter(coupon -> Boolean.TRUE.equals(coupon.getAvailable())) // 只看可用的
                    .max(Comparator.comparing(UserCouponVO::getReduceAmount))     // 比较减免金额，取最大
                    .orElse(null); // 如果没有可用的，返回 null

            // 3. 如果找到了最优券，更新 VO 和 DTO（DTO更新是为了后续分摊逻辑能拿到 ID）
            if (bestCoupon != null) {
                vo.setCouponUserRecordId(bestCoupon.getId()); // 设置返回给前端的选中ID
                vo.setCouponAmount(bestCoupon.getReduceAmount()); // 设置总优惠金额
                couponAmount = bestCoupon.getReduceAmount();

                // 重要：为了让后面的“分摊逻辑”能正常运行，我们需要把这个自动选中的 ID 赋给临时变量
                // 这样后续的分摊计算就会基于这张“最优券”进行
                dto.setCouponUserRecordId(bestCoupon.getId()); //这里的id是领取记录id
            } else {
                vo.setCouponAmount(BigDecimal.ZERO);
                couponAmount = BigDecimal.ZERO;
            }
        }
        else {
            // 如果用户手动传了 ID，则按你之前的逻辑：根据 ID 查询金额并计算
            vo.setCouponUserRecordId(dto.getCouponUserRecordId());
            //计算当前优惠券的优惠金额（包含锁定逻辑）
            BigDecimal manualAmount = couponService.calculateDiscountAmount(dto.getCouponUserRecordId(), goodsAmount);
            couponAmount = manualAmount;
            vo.setCouponAmount(manualAmount);
        }
        // 调用分摊逻辑
        this.distributeCouponAmount(vo);


        // 4. 组装最终 VO
        //设置用户默认地址
        vo.setUserAddressId(userAddressMapper.selectDefaultAddress(userId).getId());
        if (dto.getAddressId() != null) {
            vo.setUserAddressId(dto.getAddressId());
        }

        // 最终支付 = 总原价 + 总运费 - 优惠券
        vo.setPayAmount(goodsAmount.add(totalFreight).subtract(couponAmount));

        // 5. 将 Token 放入 vo 返回给前端
        vo.setSubmitToken(token);


        return vo;
    }


    /**
     * 提交订单
     * @param dto
     * @return
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public OrderCreateVO submitOrder(OrderSubmitDTO dto) {
        // 获取当前用户ID (假设你从 SecurityContext 或拦截器中获取)
        Long userId = UserHolder.getUser().getId();
        String submitToken = dto.getSubmitToken();

        // --- 0. 判断防重token是否已经删除 ---
        // --- 0. 判断并删除防重标记 (原子操作) ---
        // 构造预下单时存入的 Redis Key
        String redisKey = RedisConstant.ORDER_SUBMIT_KEY + userId + ":" + submitToken;

        // 直接执行删除。如果 Key 存在并删除成功，返回 true；如果已经被删过或已过期，返回 false。
        Boolean isFirstSubmit = stringRedisTemplate.delete(redisKey);

        if (Boolean.FALSE.equals(isFirstSubmit)) {
            // 说明该 Token 已经失效或被其他并发请求处理了
            throw new BusinessException("请勿重复提交订单，或页面已失效，请刷新后重试");
        }


        // --- 1. 计算实付金额并记录状态 ---
        OrderActualResult actualResult = this.calculateActualAmount(dto);
        BigDecimal expectedPayAmount = dto.getExpectedPayAmount();

        // 定义一个标记，记录价格是否发生变动
        boolean isPriceChanged = actualResult.getPayAmount().compareTo(expectedPayAmount) != 0;
        String changeReason = isPriceChanged ? "订单价格已更新！" : null;



        //2.生成订单编号、封装订单信息并入库
        // A. 生成唯一订单号 (orderSn)
        // 建议：可以使用 "日期" + "雪花ID后几位" 或直接用雪花ID字符串，增加可读性
        Long orderId = IdWorker.getId(); // MyBatis-Plus 自带的工具类
        // 格式：20260331 + 雪花ID后10位
        String datePrefix = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String snowflakeIdStr = String.valueOf(orderId);
        // 截取雪花ID的后10位，防止单号过长 主订单编号
        String orderSn = datePrefix + snowflakeIdStr.substring(snowflakeIdStr.length() - 10);


        // 1. 计算时间
        LocalDateTime deadline = LocalDateTime.now().plusMinutes(15);



        // --- 2. 封装 父订单 实体 ---
        ParentOrder parentOrder = ParentOrder.builder()
                // 主键与业务单号
                .id(orderId)
                .orderSn(orderSn)
                // 用户信息
                .userId(userId) // 建议从上下文中获取当前登录用户ID
                // 金额信息 (核心：使用后端重算的权威数据)
                .goodsAmount(actualResult.getGoodsAmount())   // 商品总原价
                .payAmount(actualResult.getPayAmount())       // 最终实付金额（含运费及优惠后）
                .couponAmount(actualResult.getCouponAmount()) // 优惠券抵扣金额

                // 优惠券关联
                .couponUserRecordId(dto.getCouponUserRecordId())

                // 状态初始化
                .status(0) // 0 -> 待付款

                // 支付相关 (预下单时的选择)
                .paymentType(dto.getPaymentType())
                .payDeadline(deadline)

                // 逻辑删除
                .isDelete(0) // 0-未删除
                .build();


        try {
            // 2. 构造对象与执行业务逻辑
            parentOrderMapper.insert(parentOrder);

            // 3. 其他操作（如扣减库存等）
        } catch (DuplicateKeyException e) {
            // 3. 兜底捕获 (最后一道防线)
            // 此时大概率是 ID 生成冲突或极端的并发逻辑漏洞
            log.error("数据库唯一索引冲突！OrderSn: {}, UserId: {}", parentOrder.getOrderSn(), userId);
            throw new BusinessException("该订单已存在，请勿重复下单！");
        }


        // --- 2. 封装 子订单 实体 ---
        // 1.查询地址
        Long addressId = dto.getAddressId();
        UserAddress userAddress = userAddressMapper.selectAddressById(addressId);
        if (userAddress == null) throw new BusinessException("收货地址不存在");


        // 1. 批量查询 SKU 信息 (保持不变)
        List<Long> allSkuIds = dto.getShops().stream()
                .flatMap(shop -> shop.getItems().stream())
                .map(OrderSubmitDTO.ItemSubmitRequest::getSkuId)
                .distinct()
                .collect(Collectors.toList());
        Map<Long, Sku> skuMap = skuService.getSkuMapByIds(allSkuIds);

        // 在循环开始前，定义一个全局的 subOrderList 集合
        List<SubOrder> subOrderList = new ArrayList<>();

        // 在循环开始前，定义一个全局的 OrderItem 集合
        List<OrderItem> allOrderItems = new ArrayList<>();

        // 2. 遍历店铺进行拆单
        for (OrderSubmitDTO.ShopOrderSubmitRequest shopReq : dto.getShops()) {

            // 1. 生成子订单主键 ID (数据库唯一标识)
            Long subOrderId = IdWorker.getId();

            // 2. 生成 4 位随机数后缀 (范围 1000 - 9999)
            // 使用 ThreadLocalRandom 性能更高且线程安全
            int randomSuffix = ThreadLocalRandom.current().nextInt(1000, 10000);

            // 3. 拼接子订单业务号：主订单 Sn + 随机后缀
            // 示例：20260331xxxxxxxxxx + 8829 = 20260331xxxxxxxxxx8829
            String subOrderSn = orderSn + randomSuffix;

            // 3. 构建子订单实体
            SubOrder subOrder = SubOrder.builder()
                    .id(subOrderId)
                    .parentOrderId(parentOrder.getId())
                    .parentOrderSn(parentOrder.getOrderSn())
                    .subOrderSn(subOrderSn)
                    .userId(userId)
                    .shopId(shopReq.getShopId())
                    .remark(shopReq.getShopRemark())
                    .payDeadline(deadline)
                    .paymentType(dto.getPaymentType())

                    // 状态
                    .status(0)
                    // 地址快照 (核心：冗余存储)
                    .receiverName(userAddress.getReceiverName())
                    .receiverPhone(userAddress.getReceiverPhone())
                    .receiverProvince(userAddress.getProvince())
                    .receiverCity(userAddress.getCity())
                    .receiverDistrict(userAddress.getDistrict())
                    .receiverDetailAddress(userAddress.getDetailAddress())
                    .isDelete(0)
                    .build();
            //封装金额相关字段
            calculateShopAmount(
                    subOrder,
                    shopReq,
                    skuMap,
                    actualResult.getGoodsAmount(),
                    actualResult.getCouponAmount()
                    );

            subOrderList.add(subOrder);


            // 5.每个子订单的orderItem依次入库
            // --- B. 封装该店铺下的所有商品明细 (OrderItem) ---
            for (OrderSubmitDTO.ItemSubmitRequest itemReq : shopReq.getItems()) {
                // 从 skuMap 中获取快照信息
                Sku sku = skuMap.get(itemReq.getSkuId());
                Spu spu = spuMapper.selectById(sku.getSpuId());
                if (sku == null) throw new BusinessException("商品信息异常");

                BigDecimal itemPrice = sku.getPrice();
                Integer qty = itemReq.getQuantity();
                BigDecimal itemTotal = itemPrice.multiply(new BigDecimal(qty));

                // 构建 OrderItem
                OrderItem orderItem = new OrderItem();
                orderItem.setId(IdWorker.getId()); // 明细主键
                orderItem.setSubOrderId(subOrderId); // 关联刚刚生成的子订单ID
                orderItem.setSubOrderSn(subOrderSn); // 关联子订单编号
                orderItem.setParentOrderSn(orderSn); // 关联主订单编号
                orderItem.setParentOrderId(parentOrder.getId()); // 关联主订单ID

                // 快照信息填充
                orderItem.setSpuId(sku.getSpuId());
                orderItem.setSkuId(sku.getId());


                orderItem.setSpuName(spu.getName()); // 假设 Sku 对象中有 spuName
                orderItem.setSkuName(sku.getSkuTitle()); // 如：黑色 256G
                orderItem.setPicUrl(spu.getMainImage());

                // 金额填充
                orderItem.setPrice(itemPrice);
                orderItem.setQuantity(qty);
                orderItem.setTotalAmount(itemTotal);

                /* * 关于优惠分摊 (couponAmount/realAmount)：
                 * 如果不需要精准分摊到每一行商品，此处可暂设为 ZERO。
                 * 如果需要，逻辑同子订单分摊：itemTotal / shopTotal * shopCouponAmount
                 */
                orderItem.setCouponAmount(BigDecimal.ZERO);
                orderItem.setRealAmount(itemTotal);

                allOrderItems.add(orderItem);
            }
        }



        // 4. 批量入库
        subOrderService.saveBatch(subOrderList);

        // 2. 插入订单项 (必须在子订单之后)
        orderItemService.saveBatch(allOrderItems);





        //5.判断商品来源，如果是购物车中的商品，则将Redis和Mysql中对应的数据删除
        cleanCartItems(userId,dto);



        //6.扣减库存
        // 将嵌套在 shops 里的所有 items 提取出来，转为 List<OrderItemDTO>
        List<OrderItemDTO> stockItems = dto.getShops().stream()
                .flatMap(shop -> shop.getItems().stream()) // 将多个店铺的商品列表合并为一个流
                .map(item -> {
                    OrderItemDTO orderItemDTO = new OrderItemDTO();
                    orderItemDTO.setSkuId(item.getSkuId());
                    orderItemDTO.setQuantity(item.getQuantity());
                    return orderItemDTO;
                })
                .collect(Collectors.toList());

        // 调用扣减库存方法
        this.deductStock(stockItems);

        //如果优惠券不为空，则锁定优惠券
        if (dto.getCouponUserRecordId() != null) {
            couponService.lockCoupon(dto.getCouponUserRecordId());
        }







        // 将 LocalDateTime 转换为 毫秒时间戳 (Long)
        Long deadlineTimestamp = deadline.atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();


        // --- 7. 注册事务提交后的 MQ 同步钩子 ---
        // 只有当数据库事务真正 Commit 成功后，才发送消息给 ES 同步服务
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    // --- A. 发送子订单 ES 同步消息（带可靠性保障）---
                    for (SubOrder subOrder : subOrderList) {
                        sendEsSyncMessage(subOrder.getId(), 1);  // type=1 表示新增
                    }

                    // B. 发送延迟关单消息（先入库再发送，使用 TTL + 死信队列方式）
                    OrderCancelMessage cancelMessage = new OrderCancelMessage(orderSn, userId);

                    // 1. 生成消息ID
                    String msgId = UUID.randomUUID().toString();

                    // 2. 先入库（source_type=0, business_type=1, status=0）
                    MqMessageLog messageLog = MqMessageLog.builder()
                            .id(msgId)
                            .sourceType(0)  // 0-生产者端
                            .businessType(1)  // 1-超时关单
                            .exchange(OrderMQConfig.DELAY_EXCHANGE)
                            .routingKey(OrderMQConfig.DELAY_ROUTING_KEY)
                            .payload(JSONUtil.toJsonStr(cancelMessage))
                            .status(0)  // 0-发送中
                            .retryCount(0)
                            .cause(null)
                            .nextRetryTime(LocalDateTime.now())  // 立即可重试
                            .build();
                    mqMessageLogMapper.insert(messageLog);



                    // 3. 构造 CorrelationData
                    MqCorrelationData correlationData = new MqCorrelationData(
                            msgId,
                            OrderMQConfig.DELAY_EXCHANGE,
                            OrderMQConfig.DELAY_ROUTING_KEY,
                            cancelMessage
                    );


                    // 4. 发送消息到延迟交换机（普通 Direct Exchange，支持 mandatory）
                    // 消息会先进入 delay.queue（TTL=15分钟），过期后自动进入 dlx.exchange → timeout.queue
                    rabbitTemplate.convertAndSend(
                            OrderMQConfig.DELAY_EXCHANGE,
                            OrderMQConfig.DELAY_ROUTING_KEY,
                            //wrongKey,
                            cancelMessage,
                            message -> {
                                message.getMessageProperties().setCorrelationId(msgId);
                                message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                                // 不需要设置 x-delay header，TTL 由队列配置决定
                                return message;
                            },
                            correlationData
                    );
                    log.info("已发送延迟关单消息到 TTL 队列，MsgId: {}, OrderSn: {}, UserId: {}, TTL={}分钟",
                            msgId, orderSn, userId, OrderMQConfig.ORDER_TIMEOUT_MS / 60000);
                    log.info("事务已提交：已发送 ES 同步消息,OrderSn: {}", orderSn);

                }
            });
        }


        //4.封装vo并返回
        return  OrderCreateVO.builder()
                .orderSn(orderSn)
                .paymentType(dto.getPaymentType())
                .priceChanged(isPriceChanged)
                .changeReason(changeReason)
                .payAmount(actualResult.getPayAmount())
                .payDeadline(deadlineTimestamp)
                .build();
    }


    /**
     * 取消订单
     * @param orderSn(主订单编号)
     * @param userId
     * @return
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean cancelOrder(String orderSn, Long userId) {
        //1.主订单及子订单的状态校验与更新
        ParentOrder parentOrder = parentOrderMapper.selectBySnAndUserId(orderSn, userId);
        if (parentOrder == null) {
            throw new BusinessException("该订单不存在");
        }

        // 幂等性设计：如果订单已经关闭（status=4），直接返回成功，不抛异常
        if (parentOrder.getStatus() == 4) {
            log.info("订单已经关闭，幂等性跳过 - 主订单号: {}", orderSn);
            return true;  // 返回 true 表示"处理成功"，消费者会 ACK
        }

        // 如果订单已支付或其他状态，说明不需要关单，也返回成功（幂等）
        if (parentOrder.getStatus() != 0) {
            log.info("订单状态为 {}，无需关单（可能已支付） - 主订单号: {}", parentOrder.getStatus(), orderSn);
            return true;  // 返回 true 表示"处理成功"，消费者会 ACK
        }

        //关闭主订单（乐观锁：只有 status=0 才会更新）
        int rows = parentOrderMapper.setStatusBySnAndUserId(orderSn, userId, 4, 0);

        // 幂等性保障：如果返回 0，说明订单已被其他线程关闭，直接返回成功
        if (rows == 0) {
            log.info("订单状态已变更（并发竞争失败），幂等性跳过 - 主订单号: {}", orderSn);
            return true;
        }

        //关闭子订单
        subOrderMapper.setStatusByParentSn(orderSn, userId, 4, 0);

        //2.回补库存
        // A. 查出该主订单下所有的订单项（OrderItem）
        // 建议在 orderItemMapper 中写一个根据 parentOrderSn 查询的方法
        List<OrderItem> orderItems = orderItemMapper.selectByParentSn(orderSn);


        // 即使 orderItems 是空的，这段代码也不会崩溃，只会得到一个空的 stockItems 列表
        List<OrderItemDTO> stockItems = orderItems.stream()
                .map(item -> new OrderItemDTO(item.getSkuId(), item.getQuantity()))
                .collect(Collectors.toList());

        // 只有当列表真的有东西时，才去调 Service
        if (!stockItems.isEmpty()) {
            skuService.rollBackStock(stockItems);
        }


        //3.优惠券解锁(如果有的话)
        Long couponUserRecordId = parentOrder.getCouponUserRecordId();
        if (couponUserRecordId != null) {
            couponService.releaseCoupon(couponUserRecordId);
        }


        //4.记录日志
        log.info("订单取消成功 - 主订单号: {}, 涉及商品种类: {} 类",
                orderSn, stockItems.size());


        // 5. 准备 ES 同步：查询该主订单下所有的子订单 ID
        List<Long> subOrderIds = subOrderMapper.selectIdsByParentSn(orderSn);

        // 6. 注册事务提交后的钩子，发送同步消息
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    for (Long subId : subOrderIds) {
                        sendEsSyncMessage(subId, 2);  // type=2 表示更新（取消/关闭）
                    }
                    log.info("订单取消事务已提交，已发送 {} 条子订单 ES 同步消息", subOrderIds.size());
                }
            });
        }


        return true;

    }



    /**
     * 查询子订单详情
     * @param orderSn（子订单编号）
     * @param userId
     * @return
     */
    @Override
    public SubOrderVO getSubOrderDetail(String orderSn, Long userId) {
        //1.查询出子订单
        // 1.1 精准查询子订单（带上 userId 保证越权安全）
        SubOrder subOrder = subOrderMapper.selectOne(new LambdaQueryWrapper<SubOrder>()
                .eq(SubOrder::getSubOrderSn, orderSn)
                .eq(SubOrder::getUserId, userId));

        // 1.2 判空校验
        if (subOrder == null) {
            throw new BusinessException("订单不存在或无权查看");
        }

        // 1.3 创建 VO 并拷贝基础属性
        SubOrderVO vo = new SubOrderVO();
        BeanUtils.copyProperties(subOrder, vo);

        // 1.4 手动映射不一致或特殊处理的字段
        vo.setSubOrderId(subOrder.getId()); // 实体类 id -> VO subOrderId
        Shop shop = shopMapper.selectById(subOrder.getShopId());
        vo.setShopName(shop.getShopName());
        vo.setShopLogo(shop.getShopLogo());

        // 1.5 状态文字转换 (建议调用你之前的 OrderStatusUtil)
        vo.setStatusDesc(OrderStatusUtil.getDesc(subOrder.getStatus()));
        vo.setPaymentTypeDesc(PaymentTypeEnum.getDescByCode(subOrder.getPaymentType()));

        // --- 2.关联商品明细 ---
        // 2.1 查询该子订单下的所有商品快照
        List<OrderItem> orderItems = orderItemMapper.selectList(new LambdaQueryWrapper<OrderItem>()
                .eq(OrderItem::getSubOrderId, subOrder.getId()));

        // 2.2 转化为 VO 内部类 OrderItemVO 列表
        // 即使 orderItems 为空，stream 也会返回空列表，不会 NPE
        List<SubOrderVO.OrderItemVO> itemVOs = orderItems.stream().map(item -> {
            SubOrderVO.OrderItemVO itemVO = new SubOrderVO.OrderItemVO();
            BeanUtils.copyProperties(item, itemVO);
            return itemVO;
        }).collect(Collectors.toList());

        // 2.3 塞入 VO
        vo.setItems(itemVOs);

        // 3. 返回结果
        return vo;
    }




    /**
     * 支付成功
     * @param orderSn（主订单）
     * @param paymentType
     * @return
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean paySuccess(String orderSn, Integer paymentType) {


            // 1. 查出订单,判断订单是否存在
            //由于需要手机扫码测试，这里直接根据订单Sn查，先不传userId
            ParentOrder parentOrder = parentOrderMapper.selectBySn(orderSn);
            SubOrder subOrder = subOrderMapper.selectByOrderSn(orderSn);
            if (parentOrder == null && subOrder == null) {
                throw new BusinessException("订单不存在");
            }
            if (parentOrder != null) {
               return parentOrderPaySuccess(orderSn,paymentType);
            }

            return subOrderPaySuccess(orderSn,paymentType);
    }


    /**
     * 主订单支付成功
     * @param parentOrderSn
     * @return
     */
    private boolean parentOrderPaySuccess(String parentOrderSn,Integer paymentType) {
        // 1. 行锁查询，保证并发安全
        ParentOrder parentOrder = parentOrderMapper.selectBySnForUpdate(parentOrderSn);

        // 2. 幂等判断：如果状态已经不是待支付(0)，说明处理过了
        if (parentOrder.getStatus() != 0) {
            return true;
        }

        // 3. 时间校验：防止超时支付导致的库存/优惠券逻辑混乱
        if (LocalDateTime.now().isAfter(parentOrder.getPayDeadline())) {
            throw new BusinessException("订单已超时！");
        }

        // 4. 一次性更新状态和支付信息
        LocalDateTime paymentTime = LocalDateTime.now();
        parentOrderMapper.updatePayInfo(parentOrderSn, OrderStatusConstant.PENDING_SHIP, paymentType, paymentTime);
        subOrderMapper.updatePayInfoByParentSn(parentOrderSn, OrderStatusConstant.PENDING_SHIP, paymentType, paymentTime);

        // 5. 核销优惠券
        if (parentOrder.getCouponUserRecordId() != null) {
            couponService.useCoupon(parentOrder.getCouponUserRecordId());
        }

        // 6. 注册事务钩子同步 ES (保持你原来的写法，很棒)
        registerEsSyncHook(parentOrderSn);

        return true;
    }



    /**
     * 子订单支付成功(默认无优惠券)
     * @param subOrderSn
     * @return
     */
    private boolean subOrderPaySuccess(String subOrderSn,Integer paymentType) {
        // 1. 行锁查询，保证并发安全
        SubOrder subOrder = subOrderMapper.selectByOrderSn(subOrderSn);

        // 2. 幂等判断：如果状态已经不是待支付(0)，说明处理过了
        if (subOrder.getStatus() != 0) {
            return true;
        }

        // 3. 时间校验：防止超时支付导致的库存/优惠券逻辑混乱
        if (LocalDateTime.now().isAfter(subOrder.getPayDeadline())) {
            throw new BusinessException("订单已超时！");
        }

        // 4. 一次性更新状态和支付信息
        LocalDateTime paymentTime = LocalDateTime.now();
        subOrderMapper.updatePayInfoBySubSn(subOrderSn, OrderStatusConstant.PENDING_SHIP, paymentType, paymentTime);


        // 5. 核心逻辑：联动检查父订单,判断父订单下的子订单是否都已经支付
        checkAndPayParentOrder(subOrder.getParentOrderSn());

        // 6. 注册事务钩子同步 ES (保持你原来的写法，很棒)
        registerEsSyncHook(subOrderSn);

        return true;
    }


    /**
     * 子订单单独支付时检查，判断父订单下的子订单是否都已经支付
     * @param parentOrderSn
     */
    private void checkAndPayParentOrder(String parentOrderSn) {
        log.info(">>>>>> 开始联动检查父订单状态: {}", parentOrderSn);
        // 查询该父订单下【非成功】状态的子订单数量
        int unPaid = subOrderMapper.selectUnpaid(parentOrderSn);

        if (unPaid == 0) {
            log.info("父订单 {} 下所有子订单已支付，更新父订单状态", parentOrderSn);
            // 更新父订单表状态
            parentOrderMapper.updateStatus(parentOrderSn, OrderStatusEnum.WAIT_DELIVERY.getCode());
            // 如果你以后给父订单建了 ES 索引，记得在这里也 update 一下
        }
    }


    /**
     * 将子订单数据同步到ES
     * @param orderSn
     */
    private void registerEsSyncHook(String orderSn) {
        // 5. 发送 MQ 消息同步 ES 状态
        // 注意：这里需要先获取该主订单下的所有子订单 ID
        List<Long> subOrderIds = new ArrayList<>();
        ParentOrder parentOrder = parentOrderMapper.selectBySn(orderSn);
        SubOrder subOrder = subOrderMapper.selectByOrderSn(orderSn);
        if (parentOrder != null) {
            subOrderIds.addAll(subOrderMapper.selectIdsByParentSn(orderSn));
        }
        else {
            subOrderIds.add(subOrder.getId());
        }
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    for (Long subId : subOrderIds) {
                        sendEsSyncMessage(subId, 2);  // type=2 表示更新
                    }
                    log.info("支付成功事务已提交，已发送 {} 条子订单同步消息", subOrderIds.size());
                }
            });
        }
    }


    /**
     * 查询订单列表(ES索引实现)
     * @param keyword
     * @param status
     * @param pageNo
     * @param pageSize
     * @return
     */
    @Override
    public PageResult<SubOrderVO> getSubOrderList(
            String keyword,
            Integer status,
            Integer pageNo,
            Integer pageSize) throws IOException {
        // 1. 获取当前用户（安全隔离）
        Long userId = UserHolder.getUser().getId();

        // 2. 构建查询条件
        Query query = Query.of(q -> q.bool(b -> {
            // --- 必须满足 (Must) ---
            b.must(m -> m.term(t -> t.field("user_id").value(userId))); // 权限锁定
            b.must(m -> m.term(t -> t.field("is_delete").value(0)));   // 逻辑删除过滤

            if (status != null) {
                b.must(m -> m.term(t -> t.field("status").value(status)));
            }

            // --- 搜索框逻辑：订单号 OR 商品名 (Should) ---
            if (StringUtils.hasText(keyword)) {
                b.must(m -> m.bool(sb -> {
                    // 概率 A：匹配订单号 (精确匹配)
                    sb.should(s -> s.term(t -> t.field("sub_order_sn").value(keyword)));

                    // 如果你想支持订单号前缀模糊查询，可以换成下面这行:
                    // sb.should(s -> s.prefix(p -> p.field("id").value(keyword)));

                    // 概率 B：匹配商品名 (Nested 嵌套全文检索)
                    sb.should(s -> s.nested(n -> n
                            .path("items")
                            .query(nq -> nq.match(mq -> mq
                                    .field("items.spu_name")
                                    .query(keyword)
                            ))
                    ));

                    // 核心：以上两个 should 必须中一个
                    sb.minimumShouldMatch("1");
                    return sb;
                }));
            }
            return b;
        }));

        // 3. 构建 SearchRequest (分页、排序、高亮)
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("sub_order_index")
                .query(query)
                .from((pageNo - 1) * pageSize)
                .size(pageSize)
                .sort(so -> so.field(f -> f.field("create_time").order(SortOrder.Desc)))
                .highlight(h -> h
                        .preTags("<em style='color:red;'>")
                        .postTags("</em>")
                        .fields(Collections.singletonList(
                                NamedValue.of("items.spu_name", HighlightField.of(hf -> hf))
                        ))
                )
        );

        // 4. 执行并解析结果
        SearchResponse<SubOrderIndexDoc> response = elasticsearchClient.search(searchRequest, SubOrderIndexDoc.class);

        List<SubOrderVO> voList = response.hits().hits().stream()
                .map(hit -> {
                    SubOrderIndexDoc doc = hit.source();
                    if (doc == null) return null;
                    SubOrderVO vo = convertToVO(doc); // 调用你原有的转换工具

                    // 处理商品名高亮
                    if (hit.highlight() != null && hit.highlight().containsKey("items.spu_name")) {
                        String highlightedName = hit.highlight().get("items.spu_name").get(0);
                        if (!vo.getItems().isEmpty()) {
                            vo.getItems().get(0).setSpuName(highlightedName);
                        }
                    }
                    return vo;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return new PageResult<>(response.hits().total().value(), voList);
    }


    /**
     * 子订单确认收货
     * @param subOrderSn
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void confirmReceive(String subOrderSn) {
        // 1. 安全校验：获取当前登录用户
        Long userId = UserHolder.getUser().getId();

        // 2. 查询子订单详情（校验归属权）
        SubOrder subOrder = subOrderMapper.selectByOrderSn(subOrderSn);

        if (subOrder == null || !Objects.equals(subOrder.getUserId(), userId)) {
            throw new BusinessException("订单不存在或无权操作");
        }

        // 3. 幂等校验：如果已经是成功状态，直接返回
        if (OrderStatusEnum.SUCCESS.getCode().equals(subOrder.getStatus())) {
            return;
        }

        // 4. 更新数据库子订单状态
        int row = subOrderMapper.updateStatus(
                subOrderSn,
                OrderStatusEnum.SUCCESS.getCode());
        if (row == 0) {
            throw new BusinessException("确认收货失败，请稍后重试");
        }

        SubOrder subOrder1 = subOrderMapper.selectByOrderSn(subOrderSn);
        Long subOrderId = subOrder1.getId();

        // 6. 核心逻辑：联动检查父订单
        checkAndCompleteParentOrder(subOrder.getParentOrderSn());

        // 5. 【新增逻辑】发送 MQ 消息异步同步 ES（带可靠性保障）
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    sendEsSyncMessage(subOrderId, 2);  // type=2 表示更新
                    log.info("确认收货事务已提交，已发出 ES 同步消息，子订单ID: {}", subOrderId);
                }
            });
        }
    }


    /**
     * 查询主订单状态
     * @param parentOrderSn
     */
    @Override
    public Integer getParentOrderStatus(String parentOrderSn) {
        return parentOrderMapper.selectStatus(parentOrderSn);
    }

    /**
     * 查询子订单状态
     * @param subOrderSn
     */
    @Override
    public Integer getSubOrderStatus(String subOrderSn) {
        return subOrderMapper.selectStatus(subOrderSn);
    }



    /**
     * 查询主订单详情
     * @param parentOrderSn（子订单编号）
     * @param userId
     * @return
     */
    @Override
    public List<SubOrderVO> getParentOrderDetail(String parentOrderSn, Long userId) {
            // 1. 获取所有子订单的编号 (注意：这里建议 mapper 返回的是 Sn 列表，以便匹配你的 getSubOrderDetail 方法)
            List<String> subOrderSns = subOrderMapper.selectSnByParentSn(parentOrderSn);

            // 2. 如果列表为空，直接返回空集合，防止空指针或无效循环
            if (CollectionUtils.isEmpty(subOrderSns)) {
                return new ArrayList<>();
            }

            // 3. 使用 Stream 流调用当前类的方法
            List<SubOrderVO> detailList = subOrderSns.stream()
                    .map(sn -> getSubOrderDetail(sn, userId)) // 循环调用
                    .filter(Objects::nonNull)                // 过滤掉可能查询失败的空结果
                    .collect(Collectors.toList());           // 组装成 List

            return detailList;
    }


    /**
     * 继续支付（子订单）
     * @param subOrderSn
     * @return
     */
    @Override
    public OrderCreateVO resumePay(String subOrderSn) {

        SubOrder subOrder = subOrderMapper.selectByOrderSn(subOrderSn);
        //判断主是否有使用优惠券，如果有则需要一起支付
        ParentOrder parentOrder = parentOrderMapper.selectBySn(subOrder.getParentOrderSn());
        if (parentOrder.getCouponUserRecordId() == null) {
            return buildVoAllowedSplit(subOrderSn);
        }
        else return buildVoProhibitedSplit(subOrderSn);
    }




    /**
     * 子订单支付成功
     * @param subOrderSn
     * @param paymentType
     * @return
     */
    @Override
    public boolean payResumeSuccess(String subOrderSn, Integer paymentType) {
        //Long userId = UserHolder.getUser().getId();

        // 1. 查出订单
        //由于需要手机扫码测试，这里直接根据订单Sn查，先不传userId
        SubOrder subOrder = subOrderMapper.selectByOrderSn(subOrderSn);


        // 2. 状态判断（幂等性保护）
        if (subOrder == null) {
            throw new BusinessException("订单不存在");
        }


        if (subOrder.getStatus() != 0) {
            log.warn("订单 {} 状态已变更为 {}，跳过支付逻辑", subOrder, subOrder.getStatus());
            return true; // 已经处理过了，直接回成功
        }

        // 3. 修改主订单和子订单状态
        //这里由于要用手机扫码测试，没有token，就先直接设置订单状态了，不用userId；
        subOrderMapper.setStatusBySubOrderSn(subOrderSn, OrderStatusConstant.PENDING_SHIP);


        //修改子订单支付类型和时间
        LocalDateTime paymentTime = LocalDateTime.now();
        subOrderMapper.setPaymentType(subOrderSn,paymentType,paymentTime);



        //4.核销优惠券(如果有使用优惠券)
        //如果单独支付子订单优惠券这里先释放掉
        ParentOrder parentOrder = parentOrderMapper.selectBySn(subOrder.getParentOrderSn());
        couponService.releaseCoupon(parentOrder.getCouponUserRecordId());


        // 5. 发送 MQ 消息同步 ES 状态（带可靠性保障）
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    sendEsSyncMessage(subOrder.getId(), 2);  // type=2 表示更新
                    log.info("支付成功事务已提交，已发送1条子订单同步消息");
                }
            });
        }


        log.info("💰 订单 {} 支付成功！", subOrderSn);
        return true;
    }




    /**
     * 处理单个店铺的逻辑
     */
    private OrderConfirmVO.ConfirmSubOrderVO processShopOrder(OrderConfirmDTO.ShopOrderRequest shopReq) {
        OrderConfirmVO.ConfirmSubOrderVO subVO = new OrderConfirmVO.ConfirmSubOrderVO();

        // 1. 查询店铺基本信息
        Shop shop = shopMapper.selectById(shopReq.getShopId());
        subVO.setShopId(shop.getId());
        subVO.setShopName(shop.getShopName());
        subVO.setShopLogo(shop.getShopLogo());

        // 2. 将 ItemRequest 列表转换为 ConfirmItemVO 列表
        List<OrderConfirmVO.ConfirmItemVO> itemVOs = shopReq.getItems().stream()
                .map(this::processItem)
                .collect(Collectors.toList());

        // 3. 统计该店铺的商品总价(把每一个商品的总价提取出来再做累加)
        BigDecimal shopTotal = itemVOs.stream()
                .map(OrderConfirmVO.ConfirmItemVO::getTotalPrice)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        subVO.setItems(itemVOs);
        subVO.setSubTotalAmount(shopTotal);

        // 4. 计算运费 (简单示例，实际可调用运费模版)
        BigDecimal freight = BigDecimal.TEN;
        subVO.setSubFreightAmount(freight);

        //店铺分摊到的优惠和最后的实付金额，最后处理
        return subVO;
    }


    /**
     * 处理单个商品的详情与金额
     */
    private OrderConfirmVO.ConfirmItemVO processItem(OrderConfirmDTO.ItemRequest itemReq) {
        Sku sku = skuMapper.selectById(itemReq.getSkuId());
        Spu spu = spuMapper.selectById(sku.getSpuId());

        OrderConfirmVO.ConfirmItemVO itemVO = new OrderConfirmVO.ConfirmItemVO();
        itemVO.setSkuId(sku.getId());
        itemVO.setSpuId(spu.getId());
        itemVO.setSpuName(spu.getName());
        itemVO.setSkuName(sku.getSkuTitle());
        itemVO.setPicUrl(spu.getMainImage());
        itemVO.setPrice(sku.getPrice());
        itemVO.setQuantity(itemReq.getQuantity());

        // 计算单项总价
        itemVO.setTotalPrice(sku.getPrice().multiply(new BigDecimal(itemReq.getQuantity())));

        return itemVO;
    }

    /**
     * 将总优惠金额分摊到各个店铺（子订单），并计算每个店铺的实付金额
     * 逻辑：按店铺商品总额比例分摊，并处理 0.01 元的舍入误差
     */
    private void distributeCouponAmount(OrderConfirmVO vo) {
        BigDecimal totalCoupon = vo.getCouponAmount();
        BigDecimal totalProductAmount = vo.getGoodsAmount();
        List<OrderConfirmVO.ConfirmSubOrderVO> subOrders = vo.getSubOrders();

        // 1. 安全校验：如果没有优惠或没有订单，则调用reset方法
        if (totalCoupon == null || totalCoupon.compareTo(BigDecimal.ZERO) <= 0) {
            resetSubCouponAmount(subOrders);
            return;
        }

        // 2. 准备分摊变量
        BigDecimal distributedSum = BigDecimal.ZERO; // 已分摊的总和

        // 3. 开始循环分摊
        for (int i = 0; i < subOrders.size(); i++) {
            OrderConfirmVO.ConfirmSubOrderVO subOrder = subOrders.get(i);
            BigDecimal currentShopCoupon;

            // 判断是否为最后一个店铺（用于抹平误差）
            if (i < subOrders.size() - 1) {
                // 公式：(该店总额 / 总额) * 总优惠
                // setScale(2, RoundingMode.HALF_UP) 确保金额符合财务规范（两位小数，四舍五入）
                currentShopCoupon = subOrder.getSubTotalAmount()
                        .multiply(totalCoupon)
                        .divide(totalProductAmount, 2, RoundingMode.HALF_UP);

                distributedSum = distributedSum.add(currentShopCoupon);
            } else {
                // 最后一个店铺：总优惠 - 前面所有店分掉的
                currentShopCoupon = totalCoupon.subtract(distributedSum);
            }

            // 4. 更新子订单金额
            subOrder.setSubCouponAmount(currentShopCoupon);

            // 计算实付：原价 + 运费 - 优惠（需确保不为负数）
            BigDecimal payAmount = subOrder.getSubTotalAmount()
                    .add(subOrder.getSubFreightAmount())
                    .subtract(currentShopCoupon);

            subOrder.setSubPayAmount(payAmount.compareTo(BigDecimal.ZERO) < 0 ? BigDecimal.ZERO : payAmount);
        }
    }

    /**
     * 兜底方法：重置所有子订单的优惠信息
     */
    private void resetSubCouponAmount(List<OrderConfirmVO.ConfirmSubOrderVO> subOrders) {
        for (OrderConfirmVO.ConfirmSubOrderVO sub : subOrders) {
            // 即使没有优惠，也要显式设置为 0，不能让前端看到 null
            sub.setSubCouponAmount(BigDecimal.ZERO);

            // 计算实付金额：原价 + 运费
            BigDecimal subTotal = sub.getSubTotalAmount() != null ? sub.getSubTotalAmount() : BigDecimal.ZERO;
            BigDecimal subFreight = sub.getSubFreightAmount() != null ? sub.getSubFreightAmount() : BigDecimal.ZERO;

            sub.setSubPayAmount(subTotal.add(subFreight));
        }
    }



    /**
     * 核心逻辑：基于提交的 DTO 重新计算总价、实付、优惠金额
     * 用于与 dto.getExpectedPayAmount() 进行比对
     */
    private OrderActualResult calculateActualAmount(OrderSubmitDTO dto) {
        // 1. 批量查询 SKU 信息 (保持不变)
        List<Long> allSkuIds = dto.getShops().stream()
                .flatMap(shop -> shop.getItems().stream())
                .map(OrderSubmitDTO.ItemSubmitRequest::getSkuId)
                .distinct()
                .collect(Collectors.toList());
        Map<Long, Sku> skuMap = skuService.getSkuMapByIds(allSkuIds);

        // 2. 计算商品总原价 (TotalAmount)
        BigDecimal totalProductAmount = BigDecimal.ZERO;
        for (OrderSubmitDTO.ShopOrderSubmitRequest shop : dto.getShops()) {
            for (OrderSubmitDTO.ItemSubmitRequest item : shop.getItems()) {
                Sku sku = skuMap.get(item.getSkuId());
                if (sku == null) throw new BusinessException("部分商品已下架，请重新下单");

                BigDecimal itemSum = sku.getPrice().multiply(new BigDecimal(item.getQuantity()));
                totalProductAmount = totalProductAmount.add(itemSum);
            }
        }

        // 3. 计算总运费
        BigDecimal totalFreight = new BigDecimal(dto.getShops().size()).multiply(new BigDecimal("10.00"));

        // 4. 计算优惠券抵扣金额
        BigDecimal couponAmount = BigDecimal.ZERO;
        if (dto.getCouponUserRecordId() != null) {
            couponAmount = couponService.calculateDiscountAmount(dto.getCouponUserRecordId(), totalProductAmount);
        }

        // 5. 计算最终实付金额
        BigDecimal actualPayAmount = totalProductAmount.add(totalFreight).subtract(couponAmount);
        actualPayAmount = actualPayAmount.compareTo(BigDecimal.ZERO) < 0 ? BigDecimal.ZERO : actualPayAmount;

        // 6. 封装并返回结果对象
        return OrderActualResult.builder()
                .goodsAmount(totalProductAmount)
                .couponAmount(couponAmount)
                .freightAmount(totalFreight)
                .payAmount(actualPayAmount)
                .build();
    }


    /**
     * 封装子订单的费用字段：子订单总额、子订单运费、子订单分摊到的优惠、子订单实付
     * @param subOrder
     * @param shop
     * @param skuMap
     * @param globalTotal
     * @param globalCoupon
     */
    private void calculateShopAmount(
            SubOrder subOrder,
            OrderSubmitDTO.ShopOrderSubmitRequest shop,
            Map<Long, Sku> skuMap,      // 传入 SKU 映射表
            BigDecimal globalTotal,     // 主订单总原价
            BigDecimal globalCoupon     // 主订单总优惠
    ) {
        // 1. 计算该店铺商品原价总额 (totalAmount)
        BigDecimal shopTotal = BigDecimal.ZERO;
        for (OrderSubmitDTO.ItemSubmitRequest item : shop.getItems()) {
            Sku sku = skuMap.get(item.getSkuId());
            if (sku == null) throw new BusinessException("商品信息不存在: " + item.getSkuId());

            // 单价 * 数量
            BigDecimal itemSum = sku.getPrice().multiply(new BigDecimal(item.getQuantity()));
            shopTotal = shopTotal.add(itemSum);
        }
        subOrder.setGoodsAmount(shopTotal);

        // 2. 设置该店运费 (这里暂时写死，实际可对接运费模板)
        BigDecimal freight = BigDecimal.TEN;
        subOrder.setFreightAmount(freight);

        // 3. 计算该店铺分摊到的优惠 (couponAmount)
        // 公式：该店优惠 = 总优惠 * (该店原价 / 总原价)
        BigDecimal shopCoupon = BigDecimal.ZERO;
        if (globalTotal.compareTo(BigDecimal.ZERO) > 0) {
            shopCoupon = globalCoupon.multiply(shopTotal)
                    .divide(globalTotal, 2, RoundingMode.DOWN); // 向下取整，防止超支
        }
        subOrder.setCouponAmount(shopCoupon);

        // 4. 计算该店铺最终实付 (payAmount)
        // 公式：实付 = 原价 - 优惠 + 运费
        BigDecimal shopPay = shopTotal.subtract(shopCoupon).add(freight);
        subOrder.setPayAmount(shopPay.compareTo(BigDecimal.ZERO) < 0 ? BigDecimal.ZERO : shopPay);
    }



    /**
     * 在购物车中已下单的商品（Redis和Mysql都要清理）
     * @param userId 用户ID
     * @param dto    包含店铺和商品信息的提交对象
     */
    private void cleanCartItems(Long userId, OrderSubmitDTO dto) {
        // 1. 判断来源，非购物车来源直接跳过
        if (!Objects.equals(dto.getSource(), 2)) {
            return;
        }

        // 2. 提取本次下单的所有 SKU ID
        List<Long> skuIds = dto.getShops().stream()
                .flatMap(shop -> shop.getItems().stream())
                .map(OrderSubmitDTO.ItemSubmitRequest::getSkuId)
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(skuIds)) {
            return;
        }

        // 3. 清理 Redis 缓存
        // 假设你的购物车 Key 类似 cart:1001，存储结构是 Hash (Field: skuId, Value: cartInfo)
        try {
            String cartRedisKey = RedisConstant.CART_CACHE_KEY + userId;
            // 将 List<Long> 转为 String[] 适配 Redis 的 hdel 参数
            Object[] fields = skuIds.stream().map(String::valueOf).toArray();
            Long deletedCount = stringRedisTemplate.opsForHash().delete(cartRedisKey, fields);
            // 记录日志：打印本次操作的用户、ID列表以及实际删除的数量
            log.info("清理购物车Redis记录成功 | 用户ID: {} | 尝试清理SKUs: {} | 实际移除数量: {}",
                    userId, skuIds, deletedCount);
        } catch (Exception e) {
            // Redis 失败通常不抛出异常中断下单，记录日志即可，以 MySQL 为准
            log.error("清理用户[{}]购物车Redis数据失败: {}", userId, e.getMessage());
        }

        // 4. 清理 MySQL 数据库 (核心逻辑，靠事务保证一致性)
        int rows = cartItemMapper.deleteByUserIdAndSkuIds(userId, skuIds);
        log.info("清理用户[{}]购物车数据库记录共 {} 条", userId, rows);
    }


    /**
     * 批量扣减库存
     * @param items 下单的商品明细列表
     */
    //@Transactional(rollbackFor = Exception.class) 外层加了注解，这里就不加了
    public void deductStock(List<OrderItemDTO> items) {
        if (CollectionUtils.isEmpty(items)) {
            return;
        }

        // 1. 关键步骤：按 skuId 从小到大排序，防止并发死锁
        List<OrderItemDTO> sortedItems = items.stream()
                .sorted(Comparator.comparing(OrderItemDTO::getSkuId))
                .collect(Collectors.toList());

        // 2. 遍历执行扣减
        for (OrderItemDTO item : sortedItems) {
            Long skuId = item.getSkuId();
            Integer quantity  = item.getQuantity();

            // 3. 执行原子扣减 SQL
            int rows = skuMapper.deductStock(skuId, quantity);

            // 4. 判断结果：如果影响行数为 0，说明库存不足
            if (rows == 0) {
                // 抛出异常，触发 @Transactional 事务回滚，
                // 之前循环中扣减成功的库存和已入库的订单都会自动还原。
                throw new BusinessException("商品库存不足，请重新下单");
            }
        }
    }


    /**
     * 数据转换逻辑：SubOrderIndexDTO -> SubOrderVO
     */
    private SubOrderVO convertToVO(SubOrderIndexDoc dto) {
        if (dto == null) return null;
        SubOrderVO vo = new SubOrderVO();

        // 1. 拷贝第一层简单属性 (id, status, shopName 等)
        BeanUtils.copyProperties(dto, vo);

        vo.setSubOrderId(dto.getSubOrderId());



        // 2. 手动补全第一层的金额 (Double -> BigDecimal)
        if (dto.getGoodsAmount() != null) vo.setGoodsAmount(BigDecimal.valueOf(dto.getGoodsAmount()));
        if (dto.getPayAmount() != null) vo.setPayAmount(BigDecimal.valueOf(dto.getPayAmount()));
        if (dto.getFreightAmount() != null) vo.setFreightAmount(BigDecimal.valueOf(dto.getFreightAmount()));
        if (dto.getCouponAmount() != null) vo.setCouponAmount(BigDecimal.valueOf(dto.getCouponAmount()));

        // 3. 手动补全状态文字
        vo.setStatusDesc(OrderStatusEnum.getDescByCode(dto.getStatus()));
        vo.setPaymentTypeDesc(PaymentTypeEnum.getDescByCode(dto.getPaymentType()));


        // 4. 转换内部的 Items 列表 (核心修复点)
        if (dto.getItems() != null) {
            List<SubOrderVO.OrderItemVO> itemVos = dto.getItems().stream().map(item -> {
                SubOrderVO.OrderItemVO itemVo = new SubOrderVO.OrderItemVO();

                // 这里建议也手动赋值，或者确保名字完全对上
                itemVo.setSpuId(item.getSpuId());
                itemVo.setSkuId(item.getSkuId());
                itemVo.setSpuName(item.getSpuName());
                itemVo.setSkuName(item.getSkuName());
                itemVo.setPicUrl(item.getPicUrl());
                itemVo.setQuantity(item.getQuantity());

                // ✅ 关键：手动处理 Price 的类型转换
                if (item.getPrice() != null) {
                    itemVo.setPrice(BigDecimal.valueOf(item.getPrice()));
                }

                return itemVo;
            }).collect(Collectors.toList());
            vo.setItems(itemVos);
        }
        return vo;
    }


    /**
     * 检查父订单下所有子订单状态，若全完成则关闭父订单
     */
    private void checkAndCompleteParentOrder(String parentOrderSn) {
        log.info(">>>>>> 开始联动检查父订单状态: {}", parentOrderSn);
        // 查询该父订单下【非成功】状态的子订单数量
        int unSuccessOrder = subOrderMapper.selectUnSuccess(parentOrderSn);

        if (unSuccessOrder == 0) {
            log.info("父订单 {} 下所有子订单已完成，更新父订单状态", parentOrderSn);
            // 更新父订单表状态
            parentOrderMapper.updateStatus(parentOrderSn, OrderStatusEnum.SUCCESS.getCode());
            // 如果你以后给父订单建了 ES 索引，记得在这里也 update 一下
        }
    }


    /**
     * 允许订单分开支付(主订单未使用优惠券)
     * @param subOrderSn
     * @return
     */
    private OrderCreateVO buildVoAllowedSplit(String subOrderSn) {
        SubOrder subOrder = subOrderMapper.selectByOrderSn(subOrderSn);
        OrderCreateVO vo = new OrderCreateVO();
        vo.setAllowSplitPayment(true);
        vo.setOrderSn(subOrderSn);
        vo.setPaymentType(subOrder.getPaymentType());
        vo.setPayAmount(subOrder.getPayAmount());

        LocalDateTime payDeadline = subOrder.getPayDeadline();
        // 将 LocalDateTime 转换为 毫秒时间戳 (Long)
        Long deadlineTimestamp = payDeadline.atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        vo.setPayDeadline(deadlineTimestamp);
        return vo;
    }


    /**
     * 禁止主子订单分开支付（返回主订单信息及标识）
     * @param subOrderSn
     * @return
     */
    private OrderCreateVO buildVoProhibitedSplit(String subOrderSn) {
        SubOrder subOrder = subOrderMapper.selectByOrderSn(subOrderSn);
        ParentOrder parentOrder = parentOrderMapper.selectBySn(subOrder.getParentOrderSn());
        OrderCreateVO vo = new OrderCreateVO();
        vo.setOrderSn(parentOrder.getOrderSn());
        vo.setAllowSplitPayment(false);
        vo.setPaymentType(parentOrder.getPaymentType());
        vo.setPayAmount(parentOrder.getPayAmount());

        //说明该主订单下包含哪些子订单
        List<String> Sns = subOrderMapper.selectSnsByParentSn(parentOrder.getOrderSn());
        vo.setSubOrderSns(Sns);

        LocalDateTime payDeadline = subOrder.getPayDeadline();
        // 将 LocalDateTime 转换为 毫秒时间戳 (Long)
        Long deadlineTimestamp = payDeadline.atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        vo.setPayDeadline(deadlineTimestamp);
        return vo;
    }


    /**
     * 订单实际金额
     */
    @Data
    @Builder
    public static class OrderActualResult {
        private BigDecimal goodsAmount;   // 商品总原价
        private BigDecimal couponAmount;  // 优惠券抵扣金额
        private BigDecimal freightAmount; // 总运费
        private BigDecimal payAmount;     // 最终实付金额
    }


    /**
     * 发送子订单 ES 同步消息（带可靠性保障）
     * 先入库日志，再发送消息，通过 ConfirmCallback 确认结果
     *
     * @param subOrderId 子订单ID
     * @param type       操作类型：1-新增，2-更新，3-删除
     */
    private void sendEsSyncMessage(Long subOrderId, Integer type) {
        OrderSyncMessage syncMessage = new OrderSyncMessage(subOrderId, type);




        // 1. 生成消息ID
        String msgId = UUID.randomUUID().toString();

        // 2. 先入库日志（businessType=2 表示子订单ES同步）
        MqMessageLog messageLog = MqMessageLog.builder()
                .id(msgId)
                .sourceType(0)  // 0-生产者端
                .businessType(2)  // 2-子订单ES同步
                .exchange(SubOrderMQConfig.SUB_ORDER_EXCHANGE)
                .routingKey(SubOrderMQConfig.SUB_ORDER_SYNC_ROUTING_KEY)
                .payload(JSONUtil.toJsonStr(syncMessage))
                .status(0)  // 0-发送中
                .retryCount(0)
                .cause(null)
                .nextRetryTime(LocalDateTime.now())
                .build();
        mqMessageLogMapper.insert(messageLog);

        // 3. 构造 CorrelationData
        MqCorrelationData correlationData = new MqCorrelationData(
                msgId,
                SubOrderMQConfig.SUB_ORDER_EXCHANGE,
                SubOrderMQConfig.SUB_ORDER_SYNC_ROUTING_KEY,
                syncMessage
        );


        //模拟路由失败
        String wrongKey = "invalidKey";

        // 4. 发送消息
        rabbitTemplate.convertAndSend(
                SubOrderMQConfig.SUB_ORDER_EXCHANGE,
                //SubOrderMQConfig.SUB_ORDER_SYNC_ROUTING_KEY,
                wrongKey,
                syncMessage,
                message -> {
                    message.getMessageProperties().setCorrelationId(msgId);
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return message;
                },
                correlationData
        );

        log.info("📝 [ES同步] 已发送同步消息，MsgId: {}, SubOrderId: {}, Type: {}", msgId, subOrderId, type);
    }

}
