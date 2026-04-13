package com.github.shangtanlin.mapper.mq;

import com.github.shangtanlin.model.entity.mq.MqMessageLog;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface MqMessageLogMapper {

    /**
     * 插入消息记录（发送前入库）
     */
    @Insert("INSERT INTO mq_message_log (id, source_type, business_type, exchange, routing_key, payload, status, retry_count, cause, next_retry_time) " +
            "VALUES (#{id}, #{sourceType}, #{businessType}, #{exchange}, #{routingKey}, #{payload}, #{status}, #{retryCount}, #{cause}, #{nextRetryTime})")
    int insert(MqMessageLog log);

    /**
     * 根据ID查询
     */
    @Select("SELECT * FROM mq_message_log WHERE id = #{id}")
    MqMessageLog selectById(String id);

    /**
     * 查询待重试的生产者端消息（定时任务用）
     * source_type=0(生产者端), status IN (0, 2, 3), next_retry_time <= now
     */
    @Select("SELECT * FROM mq_message_log WHERE source_type = 0 AND status IN (0, 2, 3) AND next_retry_time <= NOW() ORDER BY create_time ASC")
    List<MqMessageLog> selectPendingRetry();

    /**
     * 查询消费者端处理失败待人工处理的记录
     * source_type=1(消费者端), status=4(人工处理)
     */
    @Select("SELECT * FROM mq_message_log WHERE source_type = 1 AND status = 4 ORDER BY create_time DESC")
    List<MqMessageLog> selectConsumePending();

    /**
     * 查询指定业务类型的人工处理记录
     */
    @Select("SELECT * FROM mq_message_log WHERE business_type = #{businessType} AND status = 4 ORDER BY create_time DESC")
    List<MqMessageLog> selectPendingByBusinessType(@Param("businessType") Integer businessType);

    /**
     * 更新状态为成功（仅当当前状态为发送中时才更新，避免覆盖路由失败）
     */
    @Update("UPDATE mq_message_log SET status = 1 WHERE id = #{id} AND status = 0")
    int updateStatusToSuccess(@Param("id") String id);

    /**
     * 更新状态为路由失败（仅当当前状态为发送中时才更新）
     */
    @Update("UPDATE mq_message_log SET status = 2, cause = #{cause} WHERE id = #{id} AND status = 0")
    int updateStatusToRouteFail(@Param("id") String id, @Param("cause") String cause);

    /**
     * 更新状态为投递失败
     */
    @Update("UPDATE mq_message_log SET status = 3, cause = #{cause} WHERE id = #{id}")
    int updateStatusToSendFail(@Param("id") String id, @Param("cause") String cause);

    /**
     * 更新重试信息（定时任务重试时使用）
     */
    @Update("UPDATE mq_message_log SET status = #{status}, retry_count = #{retryCount}, next_retry_time = #{nextRetryTime}, cause = #{cause} WHERE id = #{id}")
    int updateRetryInfo(MqMessageLog log);

    /**
     * 标记为人工处理（重试耗尽）
     */
    @Update("UPDATE mq_message_log SET status = 4 WHERE id = #{id}")
    int markManualProcessed(String id);

    /**
     * 删除所有成功的记录（定时清理用）
     */
    @Delete("DELETE FROM mq_message_log WHERE source_type = 0 AND status = 1")
    int deleteSuccessAll();
}