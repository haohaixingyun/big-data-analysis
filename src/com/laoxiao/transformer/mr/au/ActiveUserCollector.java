package com.laoxiao.transformer.mr.au;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.laoxiao.common.GlobalConstants;
import com.laoxiao.transformer.model.dim.StatsUserDimension;
import com.laoxiao.transformer.model.dim.base.BaseDimension;
import com.laoxiao.transformer.model.value.BaseStatsValueWritable;
import com.laoxiao.transformer.model.value.reduce.MapWritableValue;
import com.laoxiao.transformer.mr.IOutputCollector;
import com.laoxiao.transformer.service.IDimensionConverter;

/**
 * 
 * @author 肖斌
 *
 */
public class ActiveUserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        // 进行强制后获取对应的值
        StatsUserDimension statsUser = (StatsUserDimension) key;
        IntWritable activeUserValue = (IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1));

        // 进行参数设置
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, activeUserValue.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, activeUserValue.get());

        // 添加到batch中
        pstmt.addBatch();
    }
}
