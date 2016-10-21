package com.laoxiao.etl.util;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.laoxiao.common.EventLogConstants;
import com.laoxiao.etl.util.IPSeekerExt.RegionInfo;
import com.laoxiao.etl.util.UserAgentUtil.UserAgentInfo;
import com.laoxiao.util.TimeUtil;

/**
 * 处理日志数据的具体工作类
 * 
 * @author 肖斌
 *
 */
public class LoggerUtil {
    private static final Logger logger = Logger.getLogger(LoggerUtil.class);
    private static IPSeekerExt ipSeekerExt = new IPSeekerExt();

    /**
     * 处理日志数据logText，返回处理结果map集合<br/>
     * 如果logText没有指定数据格式，那么直接返回empty的集合
     * 
     * @param logText
     * @return
     */
    public static Map<String, String> handleLog(String logText) {
        Map<String, String> clientInfo = new HashMap<String, String>();
        if (StringUtils.isNotBlank(logText)) {
            String[] splits = logText.trim().split(EventLogConstants.LOG_SEPARTIOR);
            if (splits.length == 4) {
                // 日志格式为: ip^A服务器时间^Ahost^A请求参数
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_IP, splits[0].trim()); // 设置ip
                // 设置服务器时间
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, String.valueOf(TimeUtil.parseNginxServerTime2Long(splits[1].trim())));
                int index = splits[3].indexOf("?");
                if (index > -1) {
                	//requestBody ->en=e_pv&p_url=http%3A%2F%2Flocalhost%3A8080%2FBIG_DATA_LOG2%2Fdemo2.jsp&p_ref=http%3A%2F%2Flocalhost%3A8080%2FBIG_DATA_LOG2%2Fdemo.jsp&tt=%E6%B5%8B%E8%AF%95%E9%A1%B5%E9%9D%A22&ver=1&pl=website&sdk=js&u_ud=D45D22CD-0AEB-4FF7-BAD6-D4701A47E081&u_mid=laoxiao&u_sd=94530125-CB6B-43A4-BE40-29CE10328E7A&c_time=1466921876137&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.3%3B%20WOW64%3B%20rv%3A47.0)%20Gecko%2F20100101%20Firefox%2F47.0&b_rst=1536*864
                    String requestBody = splits[3].substring(index + 1); // 获取请求参数，也就是我们的收集数据
                    // 处理请求参数
                    handleRequestBody(requestBody, clientInfo);
                    // 处理userAgent ,抽取浏览器信息：操作系统名字，操作系统版本，浏览器名字，版本
                    handleUserAgent(clientInfo);
                    // 处理ip地址
                    handleIp(clientInfo);
                } else {
                    // 数据格式异常
                    clientInfo.clear();
                }
            }
        }
        return clientInfo;
    }

    /**
     * 处理ip地址
     * 
     * @param clientInfo
     */
    private static void handleIp(Map<String,String> clientInfo) {
        if (clientInfo.containsKey(EventLogConstants.LOG_COLUMN_NAME_IP)) {
            String ip = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_IP);
            RegionInfo info = ipSeekerExt.analyticIp(ip);
            if (info != null) {
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, info.getCountry());
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, info.getProvince());
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_CITY, info.getCity());
            }
        }
    }

    /**
     * 处理浏览器的userAgent信息
     * 
     * @param clientInfo
     */
    private static void handleUserAgent(Map<String, String> clientInfo) {
        if (clientInfo.containsKey(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT)) {
            UserAgentInfo info = UserAgentUtil.analyticUserAgent(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT));
            if (info != null) {
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, info.getOsName());
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, info.getOsVersion());
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, info.getBrowserName());
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, info.getBrowserVersion());
            }
        }
    }

    /**
     * 处理请求参数
     * 
     * @param requestBody
     * @param clientInfo
     */
    private static void handleRequestBody(String requestBody, Map<String, String> clientInfo) {
        if (StringUtils.isNotBlank(requestBody)) {
            String[] requestParams = requestBody.split("&");
            for (String param : requestParams) {
                if (StringUtils.isNotBlank(param)) {
                    int index = param.indexOf("=");
                    if (index < 0) {
                        logger.warn("没法进行解析参数:" + param + "， 请求参数为:" + requestBody);
                        continue;
                    }

                    String key = null, value = null;
                    try {
                        key = param.substring(0, index);
                        value = URLDecoder.decode(param.substring(index + 1), "utf-8");
                    } catch (Exception e) {
                        logger.warn("解码操作出现异常", e);
                        continue;
                    }
                    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                        clientInfo.put(key, value);
                    }
                }
            }
        }
    }
}
