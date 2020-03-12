package com.tuya.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tuya.core.HiveSQLParse;
import com.tuya.core.SparkSQLParse;
import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.TableInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.http.message.BasicHeader;
import org.junit.Test;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashSet;

public class HttpUtilsTest {
    ArrayList<BasicHeader> headers = new ArrayList<>();

    String sparkJobId = "64, 110, 111, 113, 115, 122, 141, 143, 208, 210, 225, 226, 234, 244, 283, 288, 313, 316, 776, 788, 799, 946, 960, 961, 979, 982, 984, 985, 986, 987, 992, 993, 994, 995, 996, 997, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1009, 1011, 1014, 1018, 1022, 1023, 1025, 1027, 1029, 1031, 1032, 1033, 1034, 1035, 1040, 1041, 1053, 1063, 1066, 1075, 1092, 1093, 1097, 1098, 1100, 1101, 1112, 1136, 1139, 1142, 1146, 1152, 1163, 1165, 1166, 1168, 1170, 1172, 1175, 1177, 1178, 1181, 1183, 1185, 1188, 1189, 1190, 1223, 1225, 1230, 1231, 1234, 1235, 1239, 1246, 1249, 1251, 1254, 1256, 1258, 1259, 1262, 1263, 1269, 1271, 1273, 1274, 1276, 1278, 1280, 1281, 1282, 1284, 1285, 1288, 1292, 1293, 1295, 1297, 1301, 1302, 1304, 1306, 1308, 1315, 1316, 1317, 1329, 1330, 1332, 1351, 1359, 1360, 1363, 1368, 1369, 1370, 1373, 1375, 1376, 1381, 1382, 1383, 1387, 1388, 1393, 1396, 1397, 1398, 1401, 1402, 1403, 1408, 1420, 1458, 1464, 1465, 1477, 1478, 1481, 1482, 1484, 1489, 1491, 1492, 1495, 1500, 1511, 1514, 1516, 1518, 1521, 1542" +
            ",1548, 1549, 1552, 1555, 1557, 1567, 1570, 1572, 1576, 1579, 1580, 1582, 1586, 1590, 1616, 1618, 1619, 1630, 1635, 1669, 1671, 1677, 1678, 1679, 1689, 1692, 1707, 1710, 1711, 1713, 1715, 1723, 1724, 1725, 1731, 1732, 1733, 1736, 1739, 1740, 1741, 1743, 1744, 1745, 1747, 1749, 1753, 1755, 1759, 1760, 1761, 1763, 1764, 1766, 1767, 1768, 1776, 1778, 1784, 1785, 1786, 1787, 1788, 1795, 1796, 1797, 1798, 1799, 1801, 1810, 1812, 1822, 1825, 1827, 1830, 1843, 1845, 1847, 1850, 1855, 1856, 1857, 1859, 1861, 1862, 1868, 1869, 1870, 1871, 1881, 1882, 1883, 1884, 1885, 1886, 1896, 1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1910, 1911, 1912, 1916, 1917, 1922, 1923, 1925, 1927, 1930, 1934, 1936, 1941, 1945, 1946, 1956, 1957, 1958, 1961, 1962, 1964, 1968, 1970, 1995, 1996, 1999, 2002, 2003, 2005, 2007, 2009, 2011, 2012, 2013, 2017, 2018, 2019, 2021, 2023, 2024, 2026, 2029, 2030, 2032, 2033, 2034, 2037, 2039, 2042, 2054, 2069, 2075, 2076, 2078, 2083, 2087, 2116, 2117, 2118, 2125, 2126, 2127, 2130, 2131, 2134, 2137, 2138" +
            ",2139, 2142, 2143, 2144, 2145, 2147, 2148, 2154, 2155, 2162, 2164, 2165, 2166, 2167, 2168, 2169, 2172, 2173, 2176, 2178, 2181, 2183, 2184, 2188, 2189, 2190, 2194, 2196, 2197, 2198, 2199, 2205, 2207, 2208, 2210, 2214, 2219, 2227, 2237, 2238, 2239, 2240, 2241, 2243, 2246, 2247, 2251, 2252, 2255, 2259, 2262, 2264, 2265, 2266, 2267, 2268, 2269, 2272, 2273, 2281, 2282, 2284, 2288, 2293, 2345, 2357, 2365, 2369, 2377, 2381, 2385, 2389, 2393, 2397, 2414, 2442, 2446, 2450, 2454, 2462, 2466, 2470, 2498, 2502, 2510, 2514, 2554, 2566, 2582, 2586, 2594, 2606, 2610, 2614, 2618, 2686, 2694, 2698, 2714, 2718, 2734, 2738, 2750, 2758, 2770, 2774, 2786, 2790, 2798, 2814, 2818, 2830, 2838, 2854, 2858, 2866, 2874, 2878, 2890, 2894, 2905, 2907, 2922, 2926, 2934, 2954, 2958, 2970, 2990, 2998, 3002, 3018, 3022, 3026, 3034, 3074, 3138, 3142, 3158, 3162, 3170, 3178, 3202, 3206, 3210, 3214, 3218, 3230, 3238, 3242, 3246, 3250, 3262, 3274, 3314, 3346, 3350, 3354, 3362, 3378, 3410, 3430, 3450, 3454, 3542, 3554, 3558, 3566, 3570, 3574, 3578," +
            "3586, 3594, 3602, 3622, 3630, 3634, 3642, 3658, 3662, 3714, 3718, 3734, 3738, 3758, 3762, 3766, 3786, 3794, 3818, 3822, 3826, 3838, 3850, 3882, 3886, 3910, 3914, 3922, 3926, 3930, 3934, 3938, 3942, 3946, 3954, 3966, 3970, 3978, 3982, 3986, 3990, 3994, 3998, 4006, 4078, 4090, 4198, 4214, 4234, 4286, 4346, 4430, 4446, 4466, 4486, 4490, 4494, 4498, 4502, 4526, 4534, 4542, 4574, 4578, 4582, 4590, 4594, 4602, 4606, 4610, 4614, 4618, 4674, 4682, 4686, 4690, 4694, 4698, 4706, 4710, 4718, 4722, 4742, 4746, 4750, 4758, 4766, 4798, 4806, 4810, 4814, 4818, 4826, 4850, 4862, 4878, 4886, 4894, 4902, 4906, 4910, 4942, 4946, 4966, 4970, 4974, 4978, 4982, 4990, 5010, 5014, 5018, 5034, 5042, 5050, 5062, 5090, 5102, 5114, 5118, 5122, 5126, 5162, 5166, 5174, 5182, 5198, 5206, 5214, 5234, 5238, 5242, 5254, 5266, 5270, 5286, 5298, 5302, 5306, 5310, 5314, 5330, 5338, 5350, 5354, 5358, 5366, 5370, 5378, 5394, 5402, 5406, 5410, 5414, 5450, 5454, 5462, 5466, 5470, 5474, 5478, 5502, 5506, 5518, 5526, 5530, 5542, 5550, 5566, 5574, 5590," +
            "5594, 5598, 5602, 5606, 5610, 5614, 5618, 5626, 5630, 5634, 5654, 5690";
    /**
     * hiveJobId
     */
    String hiveJobId = "1015, 1016, 1036, 1037, 1039, 1043, 1055, 1061, 1069, 1070, 1071, 1072, 1073, 1074, 1084, 1085, 1086, 1149, 1153, 1156, 1157, 1203, 1339, 1463, 1865, 1914, 2093, 2163, 2222, 3234, 3270, 3298, 3598, 4366";

    {
        headers.add(new BasicHeader("cookie",
                "_ga=GA1.2.1045647750.1571648344; 7ce0ff06556c05363a176b03dfdd5680=1160; a608ea7c4cbd1919ce039822a2e5d753=01160; cd1f6c4c522c03e21ad83ee2d7b0c515=%E8%8B%8F%E6%89%BF%E7%A5%A5%EF%BC%88%E8%8E%AB%E9%82%AA%EF%BC%89; e255ad9b8262a02d28bca48235a96357=1346; HERA_Token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzc29JZCI6IjIiLCJzc29fbmFtZSI6InN1Y3giLCJhdWQiOiIyZGZpcmUiLCJpc3MiOiJoZXJhIiwiZXhwIjoxNTg0MDA1OTI2LCJ1c2VySWQiOiIxIiwiaWF0IjoxNTgzNzQ2NzI2LCJ1c2VybmFtZSI6ImhlcmEifQ.jILyJ9EUcJ4CoD8_pOQNKIfTWCSCG0g0Rrf3amtejWU; SSO_USER_TOKEN=p_586e8567ee97e69661a1238c0efe6d56"
        ));
    }

    @Test
    public void httpSparkParse() throws SqlParseException {
        long maxCost = 0;
        long aveCost = 0;
        int cnt = 0;
        String[] split = sparkJobId.split(",");
        System.out.println(split.length);
        for (String s : split) {
            System.out.println("当前处理的任务ID:" + s);

            String jobVersion = getJobVersion(s);

            if (StringUtils.isNotBlank(jobVersion)) {
                String sql = previewCode(jobVersion);
                long start = System.currentTimeMillis();

                SparkSQLParse sparkSQLParse = new SparkSQLParse();
                Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse = sparkSQLParse.parse(sql);
                long cost = System.currentTimeMillis() - start;
                System.out.println(parse + "耗时:" + cost + "ms");
                if (cost > maxCost) {
                    maxCost = cost;
                }
                aveCost += cost;
                System.out.println("最大耗时:" + maxCost + ",平均耗时:" + (aveCost) / (++cnt));
            }
        }

    }

    @Test
    public void httpHiveParse() throws SqlParseException {
        long maxCost = 0;
        long aveCost = 0;
        int cnt = 0;
        String[] split = hiveJobId.split(",");
        System.out.println(split.length);
        for (String s : split) {
            System.out.println("当前处理的任务ID:" + s);

            String jobVersion = getJobVersion(s);

            if (StringUtils.isNotBlank(jobVersion)) {
                String sql = previewCode(jobVersion);
                long start = System.currentTimeMillis();
                HiveSQLParse hiveSQLParse = new HiveSQLParse();
                Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse = hiveSQLParse.parse(sql);
                long cost = System.currentTimeMillis() - start;
                System.out.println(parse + "耗时:" + cost + "ms");
                if (cost > maxCost) {
                    maxCost = cost;
                }
                aveCost += cost;
                System.out.println("最大耗时:" + maxCost + ",平均耗时:" + (aveCost) / (++cnt));
            }
        }

    }

    @Test
    public void doPost() {
        JSONObject object = new JSONObject();
        object.put("dp_id", "zhangsan");
        object.put("type", "zhangsan");
    }


    private String previewCode(String jobVersion) {
        String url = "https://hera-cn.tuya-inc.top:7799/scheduleCenter/previewJob.do?actionId=" + jobVersion;
        String s = HttpUtils.doGet(url, headers);

        JSONObject object = JSONObject.parseObject(s);

        return object.getString("data");


    }


    private String getJobVersion(String jobId) {


        String url = "https://hera-cn.tuya-inc.top:7799/scheduleCenter/getJobVersion.do?jobId=" + jobId;


        String s = HttpUtils.doGet(url, headers);


        JSONObject jsonObject = JSONObject.parseObject(s);
        JSONArray data = jsonObject.getJSONArray("data");

        if (data.size() == 0) {
            return null;
        }
        JSONObject o = data.getJSONObject(0);

        return o.getString("id");


    }
}
