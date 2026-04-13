package com.github.shangtanlin.model.dto.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderSubmitDTO {



    // --- 补充字段：商品来源
    private Integer source; // 1-立即购买，2-购物车


    // --- 1. 收货信息 ---
    private Long addressId;          // 最终选中的收货地址ID

    // --- 2. 优惠信息 ---
    private Long couponUserRecordId; // 最后选中的优惠券（可选）

    // --- 3. 支付与配送 ---
    private Integer paymentType;         // 支付方式：1-支付宝, 2-微信

    // 用户在确认页看到的金额快照（用于后端校验）
    private BigDecimal expectedPayAmount;   // 预期实付金额


    // --- 4. 店铺维度信息 (核心：用于拆单) ---
    private List<ShopOrderSubmitRequest> shops;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ShopOrderSubmitRequest {
        private Long shopId;           // 店铺ID
        private Long shopCouponId;     // 该店铺选中的店铺券ID（可选）
        private String shopRemark;     // 该店铺的留言/备注

        // 该店铺下的商品
        private List<ItemSubmitRequest> items;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ItemSubmitRequest {
        private Long skuId;     //如果是从购物车传入，则需要把Redis和Mysql中的数据清除
        private Integer quantity;
    }


    private String submitToken; //防重token


}
