package com.github.shangtanlin.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.github.shangtanlin.model.entity.order.ParentOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;

@Mapper
public interface ParentOrderMapper extends BaseMapper<ParentOrder> {

    /**
     * 根据编号和用户Id查询订单详情
     * @param orderSn
     * @param userId
     */
    @Select("select * from parent_order where " +
            "order_sn = #{orderSn} " +
            "and user_id = #{userId}")
    ParentOrder selectBySnAndUserId(
            @Param("orderSn") String orderSn,
            @Param("userId") Long userId);

    /**
     * 根据编号和用户Id修改订单状态（乐观锁）
     * @param orderSn
     * @param userId
     * @param status
     * @param oldStatus
     * @return 影响的行数（0 表示状态已变更，未更新）
     */
    @Update("update parent_order set status = #{status} " +
            "where order_sn = #{orderSn} and user_id = #{userId} " +
            "and status = #{oldStatus}")
    int setStatusBySnAndUserId(
            @Param("orderSn") String orderSn,
            @Param("userId") Long userId,
            @Param("status") Integer status,
            @Param("oldStatus") Integer oldStatus);


    /**
     * 根据主订单编号查询主订单
     * @param orderSn
     * @return
     */
    @Select("select * from parent_order where order_sn = #{orderSn}")
    ParentOrder selectBySn(@Param("orderSn") String orderSn);


    /**
     * 修改主订单状态
     * @param orderSn
     * @param status
     */
    @Update("update parent_order set status = #{status} " +
            "where order_sn = #{orderSn}")
    void setStatusWithoutUserId(@Param("orderSn") String orderSn,
                                @Param("status") Integer status);


    /**
     * 修改主订单支付时间和支付类型
     * @param orderSn
     * @param paymentType
     * @param paymentTime
     */
    @Update("update parent_order set payment_type = #{paymentType}, " +
            "payment_time = #{paymentTime} where order_sn = #{orderSn}")
    void setPaymentType(
            @Param("orderSn") String orderSn,
            @Param("paymentType") Integer paymentType,
            @Param("paymentTime") LocalDateTime paymentTime);


    /**
     * 修改主订单状态
     * @param orderSn
     * @param status
     */
    @Update("update parent_order set status = #{status} " +
            "where order_sn = #{orderSn}")
    void updateStatus(
            @Param("orderSn") String orderSn,
            @Param("status") Integer status);


    /**
     * 查询主订单状态
     * @param parentOrderSn
     */
    @Select("select status from parent_order " +
            "where order_sn = #{parentOrderSn}")
    Integer selectStatus(@Param("parentOrderSn") String parentOrderSn);


    /**
     * 根据编号查询主订单（加行锁）
     * @param parentOrderSn
     * @return
     */
    @Select("select * from parent_order " +
            "where order_sn = #{parentOrderSn} for update")
    ParentOrder selectBySnForUpdate(
            @Param("parentOrderSn") String parentOrderSn);


    /**
     * 更新支付相关字段
     * @param parentOrderSn
     * @param status
     * @param paymentType
     * @param paymentTime
     */
    @Update("update parent_order set status = #{status}," +
            "payment_type = #{paymentType}," +
            "payment_time = #{paymentTime} " +
            "where order_sn = #{parentOrderSn}")
    void updatePayInfo(
            @Param("parentOrderSn") String parentOrderSn,
            @Param("status") Integer status,
            @Param("paymentType") Integer paymentType,
            @Param("paymentTime") LocalDateTime paymentTime);
}
