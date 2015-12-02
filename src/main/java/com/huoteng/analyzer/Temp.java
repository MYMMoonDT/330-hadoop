package com.huoteng.analyzer;

/**
 * 考虑使用聚类的方式对数据进行分析，将每天的路径看作三维坐标
 *
 * 在Map中调用聚类的库中进行聚类，输出为当天的判断结果，哪里是家，哪里是工作地，如果是周末输出为周末常住地（包括家），key为:MSID,status
 * 在reduce中对每天的结果进行聚类，输出为最终结果
 * 控制好聚类的粒度
 * 聚类方法可能受极值影响
 * Created by teng on 11/24/15.
 */
public class Temp {
}
