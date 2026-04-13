package com.github.shangtanlin.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.github.shangtanlin.common.utils.UserHolder;
import com.github.shangtanlin.mapper.CartItemMapper;
import com.github.shangtanlin.mapper.ShopMapper;
import com.github.shangtanlin.mapper.SkuMapper;
import com.github.shangtanlin.mapper.SpuMapper;
import com.github.shangtanlin.mapper.mq.MqMessageLogMapper;
import com.github.shangtanlin.model.dto.CartItemDTO;
import com.github.shangtanlin.model.dto.mq.MqCorrelationData;
import com.github.shangtanlin.model.entity.cart.CartItem;
import com.github.shangtanlin.model.entity.mq.MqMessageLog;
import com.github.shangtanlin.model.entity.product.Sku;
import com.github.shangtanlin.model.entity.product.Spu;
import com.github.shangtanlin.model.entity.shop.Shop;
import com.github.shangtanlin.model.redis.CartRedisJson;
import com.github.shangtanlin.model.vo.CartItemVO;
import com.github.shangtanlin.mq.cart.CartWriteBackMessage;
import com.github.shangtanlin.result.Result;
import com.github.shangtanlin.service.CartService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.github.shangtanlin.common.constant.CartConstant.*;
import static com.github.shangtanlin.common.constant.RedisConstant.CART_CACHE_KEY;
import static com.github.shangtanlin.common.constant.RedisConstant.CART_EMPTY_STUB;
import static com.github.shangtanlin.config.mq.CartMQConfig.CART_EXCHANGE;
import static com.github.shangtanlin.config.mq.CartMQConfig.CART_ROUTING_KEY;

@Service
@Slf4j
public class CartServiceImpl implements CartService {

    @Autowired
    private CartItemMapper cartItemMapper;

    @Autowired
    private SpuMapper spuMapper;

    @Autowired
    private SkuMapper skuMapper;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private MqMessageLogMapper mqMessageLogMapper;

    @Autowired
    private ShopMapper shopMapper;


    @Autowired
    private RBloomFilter<String> skuBloomFilter; // 注入你配置好的布隆过滤器

    //获取购物车列表
    @Override
    public List<CartItemVO> getCartList() {
        Long userId = UserHolder.getUser().getId();
        String cartKey = CART_CACHE_KEY + userId;

        // 1. 获取 Redis 中的哈希数据
        Map<Object, Object> carts = stringRedisTemplate.opsForHash().entries(cartKey);

        if (carts.containsKey(CART_EMPTY_STUB)) {
            log.debug("触发缓存穿透拦截：用户 {} 的购物车确定为空", userId);
            return Collections.emptyList();
        }

        // 2. 缓存缺失：查库并回写
        if (CollectionUtils.isEmpty(carts)) {
            return handleCacheMiss(userId, cartKey);
        }

        // 3. 缓存命中：批量组装数据
        // 3.1 提取所有 SKU ID 并反序列化 Redis 中的基本信息（数量、勾选、加入时间）
        Map<Long, CartRedisJson> redisDataMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : carts.entrySet()) {
            String skuIdStr = entry.getKey().toString();
            String json = entry.getValue().toString();
            redisDataMap.put(Long.valueOf(skuIdStr), JSONUtil.toBean(json, CartRedisJson.class));
        }

        if (redisDataMap.isEmpty()) return Collections.emptyList();

        // 3.2 批量查询数据库：SKU -> SPU -> Shop
        List<Long> skuIds = new ArrayList<>(redisDataMap.keySet());

        // 直接调用封装好的组装方法
        return assembleCartVOList(skuIds, redisDataMap);
    }


    /**
     * 缓存未命中时的处理逻辑
     */
    private List<CartItemVO> handleCacheMiss(Long userId, String cartKey) {
        List<CartItem> cartItems = cartItemMapper.list(userId);

        if (CollectionUtils.isEmpty(cartItems)) {
            // 防穿透
            stringRedisTemplate.opsForHash().put(cartKey, CART_EMPTY_STUB, CART_EMPTY_STUB);
            stringRedisTemplate.expire(cartKey, 5, TimeUnit.MINUTES);
            return Collections.emptyList();
        }

        // 1. 准备回写数据并提取 SKU ID
        Map<String, String> redisMap = new HashMap<>();
        Map<Long, CartRedisJson> redisDataMap = new HashMap<>();
        List<Long> skuIds = new ArrayList<>();

        for (CartItem item : cartItems) {
            Long skuId = item.getSkuId();
            skuIds.add(skuId);

            CartRedisJson redisJson = new CartRedisJson();
            redisJson.setQuantity(item.getQuantity());
            redisJson.setChecked(item.getChecked());
            redisJson.setCreateTime(item.getCreateTime());

            redisDataMap.put(skuId, redisJson);
            redisMap.put(skuId.toString(), JSONUtil.toJsonStr(redisJson));
        }

        // 2. 异步或同步回写 Redis (这里演示同步)
        stringRedisTemplate.opsForHash().putAll(cartKey, redisMap);
        stringRedisTemplate.expire(cartKey, 30, TimeUnit.DAYS);

        // 3. 调用统一的组装方法
        return assembleCartVOList(skuIds, redisDataMap);
    }


    /**
     * 统一的批量组装 VO 方法 (解决 Cannot resolve method 报错)
     */
    private List<CartItemVO> assembleCartVOList(List<Long> skuIds, Map<Long, CartRedisJson> redisDataMap) {
        if (CollectionUtils.isEmpty(skuIds)) return Collections.emptyList();

        // 1. 批量查询 SKU
        List<Sku> skus = skuMapper.selectBatchIds(skuIds);
        if (CollectionUtils.isEmpty(skus)) return Collections.emptyList();

        // 2. 批量查询 SPU
        List<Long> spuIds = skus.stream().map(Sku::getSpuId).distinct().collect(Collectors.toList());
        List<Spu> spus = spuMapper.selectBatchIds(spuIds);
        Map<Long, Spu> spuMap = spus.stream().collect(Collectors.toMap(Spu::getId, s -> s));

        // 3. 批量查询 Shop
        List<Long> shopIds = spus.stream().map(Spu::getShopId).distinct().collect(Collectors.toList());
        List<Shop> shops = shopMapper.selectBatchIds(shopIds);
        Map<Long, Shop> shopMap = shops.stream().collect(Collectors.toMap(Shop::getId, s -> s));

        // 4. 内存拼接
        return skus.stream().map(sku -> {
                    Spu spu = spuMap.get(sku.getSpuId());
                    Shop shop = (spu != null) ? shopMap.get(spu.getShopId()) : null;
                    CartRedisJson redisInfo = redisDataMap.get(sku.getId());

                    CartItemVO vo = new CartItemVO();
                    BeanUtil.copyProperties(sku, vo); // 拷贝基础属性如price 等

                    if (spu != null) {
                        vo.setSpuId(spu.getId());
                        vo.setSkuId(sku.getId());
                        vo.setUserId(UserHolder.getUser().getId());
                        vo.setTitle(spu.getName() + " " + spu.getDescription());
                        vo.setImage(spu.getMainImage());
                        vo.setShopId(spu.getShopId());
                        if (shop != null) vo.setShopName(shop.getShopName());
                    }

                    if (redisInfo != null) {
                        vo.setQuantity(redisInfo.getQuantity());
                        vo.setChecked(redisInfo.getChecked());
                        vo.setCreateTime(redisInfo.getCreateTime());
                    }
                    return vo;
                })
                .sorted(Comparator.comparing(CartItemVO::getCreateTime).reversed())
                .collect(Collectors.toList());
    }


    //添加购物车
    @Override
    public Result<?> addToCart(CartItemDTO cartItemDTO) {
        String skuIdStr = cartItemDTO.getSkuId().toString();

        // 【核心拦截点】如果布隆过滤器判断没有，直接拒绝，保护后面的数据库操作
        if (!skuBloomFilter.contains(skuIdStr)) {
            return Result.fail("商品信息不存在，请刷新重试");
        }

        Long userId = UserHolder.getUser().getId();
        String cartKey = CART_CACHE_KEY + userId;

        //先删除占位符
        stringRedisTemplate.opsForHash().delete(cartKey, CART_EMPTY_STUB);

        //封装redis到json
        CartRedisJson cartRedisJson = new CartRedisJson();
        cartRedisJson.setCreateTime(LocalDateTime.now());
        cartRedisJson.setChecked(CART_DEFAULT_CHECKED);
        cartRedisJson.setQuantity(cartItemDTO.getQuantity());
        //写入数据
        stringRedisTemplate.opsForHash().put(cartKey,
                cartItemDTO.getSkuId().toString(),
                JSONUtil.toJsonStr(cartRedisJson));

        //写操作，刷新ttl
        stringRedisTemplate.expire(cartKey,30, TimeUnit.DAYS);

        //异步同步数据库
        //封装消息
        CartWriteBackMessage cartWriteBackMessage = new CartWriteBackMessage();
        cartWriteBackMessage.setCartRedisJson(cartRedisJson);
        cartWriteBackMessage.setSkuId(cartItemDTO.getSkuId());
        cartWriteBackMessage.setUserId(userId);
        cartWriteBackMessage.setType(WRITE_BACK_UPDATE);
        cartWriteBackMessage.setCreateTime(LocalDateTime.now());

        // 1. 生成唯一 ID
        String msgId = UUID.randomUUID().toString();
        cartWriteBackMessage.setMsgId(msgId);

        // 2. 【先入库再发送】发送前先入库（status=0-发送中）
        MqMessageLog messageLog = MqMessageLog.builder()
                .id(msgId)
                .sourceType(0)  // 0-生产者端
                .businessType(0)  // 0-购物车回流
                .exchange(CART_EXCHANGE)
                .routingKey(CART_ROUTING_KEY)
                .payload(JSONUtil.toJsonStr(cartWriteBackMessage))
                .status(0)  // 0-发送中
                .retryCount(0)
                .cause(null)
                .nextRetryTime(LocalDateTime.now())  // 立即可重试
                .build();
        mqMessageLogMapper.insert(messageLog);

        // 3. 准备 CorrelationData
        MqCorrelationData correlationData = new MqCorrelationData(
                msgId,
                CART_EXCHANGE,
                CART_ROUTING_KEY,
                cartWriteBackMessage
        );

        String wrongKey = "invalid_key";

        // 4. 发送消息
        rabbitTemplate.convertAndSend(
                CART_EXCHANGE,
                //CART_ROUTING_KEY,
                wrongKey,
                cartWriteBackMessage,
                message -> {
                    message.getMessageProperties().setCorrelationId(msgId);
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return message;
                },
                correlationData
        );

        //返回结果
        return Result.ok();
    }

    //删除商品
    @Override
    public Result<?> deleteFromCart(Long skuId) {
        Long userId = UserHolder.getUser().getId();
        String cartKey = CART_CACHE_KEY + userId;
        Long result = stringRedisTemplate.opsForHash().delete(cartKey, skuId.toString());
        if (result == 0) {
            return Result.fail("商品不存在");
        }
        //写操作，刷新ttl
        stringRedisTemplate.expire(cartKey, 30, TimeUnit.DAYS);

        //封装消息
        CartWriteBackMessage cartWriteBackMessage = new CartWriteBackMessage();
        cartWriteBackMessage.setUserId(userId);
        cartWriteBackMessage.setType(WRITE_BACK_DELETE);
        cartWriteBackMessage.setSkuId(skuId);
        cartWriteBackMessage.setCreateTime(LocalDateTime.now());

        // 1. 生成唯一 ID
        String msgId = UUID.randomUUID().toString();
        cartWriteBackMessage.setMsgId(msgId);

        // 2. 【先入库再发送】
        MqMessageLog messageLog = MqMessageLog.builder()
                .id(msgId)
                .sourceType(0)  // 0-生产者端
                .businessType(0)  // 0-购物车回流
                .exchange(CART_EXCHANGE)
                .routingKey(CART_ROUTING_KEY)
                .payload(JSONUtil.toJsonStr(cartWriteBackMessage))
                .status(0)
                .retryCount(0)
                .cause(null)
                .nextRetryTime(LocalDateTime.now().plusMinutes(1))
                .build();
        mqMessageLogMapper.insert(messageLog);

        // 3. 准备 CorrelationData
        MqCorrelationData correlationData = new MqCorrelationData(
                msgId, CART_EXCHANGE, CART_ROUTING_KEY, cartWriteBackMessage
        );




        // 4. 发送消息
        rabbitTemplate.convertAndSend(
                CART_EXCHANGE,
                CART_ROUTING_KEY,
                cartWriteBackMessage,
                message -> {
                    message.getMessageProperties().setCorrelationId(msgId);
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return message;
                },
                correlationData
        );

        return Result.ok();
    }

    //修改购物车
    @Override
    public Result<?> updateCart(CartItemDTO cartItemDTO) {
        //为了保持原有的时间，先取出value，并转为CartRedisJson
        Long userId = UserHolder.getUser().getId();
        String cartKey = CART_CACHE_KEY + userId;
        String skuId = cartItemDTO.getSkuId().toString();
        String json = (String) stringRedisTemplate.opsForHash().get(cartKey, skuId);
        if (StrUtil.isBlank(json)) {
            return Result.fail("商品已不存在或已失效");
        }
        CartRedisJson cartRedisJson = JSONUtil.toBean(json, CartRedisJson.class);
        cartRedisJson.setQuantity(cartItemDTO.getQuantity());
        cartRedisJson.setChecked(cartItemDTO.getChecked());
        cartRedisJson.setCreateTime(LocalDateTime.now());

        //2.重写覆盖
        stringRedisTemplate.opsForHash().put(cartKey, skuId, JSONUtil.toJsonStr(cartRedisJson));
        //写操作，刷新ttl
        stringRedisTemplate.expire(cartKey, 30, TimeUnit.DAYS);

        //封装消息
        CartWriteBackMessage cartWriteBackMessage = new CartWriteBackMessage();
        cartWriteBackMessage.setCartRedisJson(cartRedisJson);
        cartWriteBackMessage.setSkuId(cartItemDTO.getSkuId());
        cartWriteBackMessage.setUserId(userId);
        cartWriteBackMessage.setType(WRITE_BACK_UPDATE);
        cartWriteBackMessage.setCreateTime(LocalDateTime.now());

        // 1. 生成唯一 ID
        String msgId = UUID.randomUUID().toString();
        cartWriteBackMessage.setMsgId(msgId);

        // 2. 【先入库再发送】
        MqMessageLog messageLog = MqMessageLog.builder()
                .id(msgId)
                .sourceType(0)  // 0-生产者端
                .businessType(0)  // 0-购物车回流
                .exchange(CART_EXCHANGE)
                .routingKey(CART_ROUTING_KEY)
                .payload(JSONUtil.toJsonStr(cartWriteBackMessage))
                .status(0)
                .retryCount(0)
                .cause(null)
                .nextRetryTime(LocalDateTime.now().plusMinutes(1))
                .build();
        mqMessageLogMapper.insert(messageLog);

        // 3. 准备 CorrelationData
        MqCorrelationData correlationData = new MqCorrelationData(
                msgId, CART_EXCHANGE, CART_ROUTING_KEY, cartWriteBackMessage
        );

        // 4. 发送消息
        rabbitTemplate.convertAndSend(
                CART_EXCHANGE,
                CART_ROUTING_KEY,
                cartWriteBackMessage,
                message -> {
                    message.getMessageProperties().setCorrelationId(msgId);
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return message;
                },
                correlationData
        );

        return Result.ok();
    }




}
