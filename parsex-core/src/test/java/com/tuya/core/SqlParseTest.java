package com.tuya.core;

import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.TableInfo;
import org.junit.Test;
import scala.Tuple3;

import java.util.HashSet;

import static com.tuya.core.util.SqlParseUtil.print;

public class SqlParseTest {

    String sql1 = "alter table dwd_afterservice_feedback_item\n" +
            "add columns ( app_name        string comment 'app名称',\n" +
            "    app_owner       string  comment 'app拥有者')";

    String sql = "select \n" +
            "r1.order_id\n" +
            ",r2.owner_id\n" +
            ",r2.user_id\n" +
            ",r2.user_name\n" +
            ",r2.app_id\n" +
            ",r2.app_name\n" +
            ",gmt_order as \"下单日期\"\n" +
            ",service_name as \"服务名称\"\n" +
            ",specification_name as \"套餐名称\"\n" +
            ",amount_origin_cny as \"应收\"\n" +
            ",amount_paid_cny as \"实收\"\n" +
            ",customer_name as \"CRM客户名称\"\n" +
            ",bd_name as \"BD名称\"\n" +
            ",dept_full_name as \"组织架构\"\n" +
            "from\n" +
            "(select \n" +
            "*\n" +
            "from\n" +
            "(\n" +
            "    select substr(gmt_order_create,1,10) as gmt_order,\n" +
            "    service_name,\n" +
            "    specification_code,\n" +
            "    a.order_id,\n" +
            "    username,\n" +
            "    amount_origin_cny,\n" +
            "    amount_paid_cny\n" +
            "    from \n" +
            "    (select * from bi_dm.dm_2bservice_item \n" +
            "    where dt = date_format(date_add('DAY', -1, now()), '%Y%m%d')\n" +
            "    and order_status_new IN ('paid','delivered','finished')) a \n" +
            "    left join \n" +
            "    (select order_id,specification_code \n" +
            "    from bi_ods.ods_hongjun_order \n" +
            "    where dt = date_format(date_add('DAY', -1, now()), '%Y%m%d'))b \n" +
            "    on a.order_id=b.order_id\n" +
            "    group by is_third,service_name,substr(gmt_order_create,1,10),specification_code,a.order_id,username,amount_origin_cny,amount_paid_cny\n" +
            "    )x\n" +
            "    left join\n" +
            "    (select specification_code,specification_name \n" +
            "    from bi_ods.ods_hongjun_specification where dt = date_format(date_add('DAY', -1, now()), '%Y%m%d')\n" +
            "    group by specification_code,specification_name \n" +
            "    )y \n" +
            "    on x.specification_code=y.specification_code\n" +
            "    where lower(service_name) like '%app%商城%'\n" +
            ") r1\n" +
            "\n" +
            "left join\n" +
            "\n" +
            "(\n" +
            "    select t1.served_id,t1.mall_code,t1.owner_id,t2.order_id,t2.user_id,t2.user_name,t3.biz_type,t3.app_id,t4.app_name from \n" +
            "    (select * from bi_ods.ods_sellercenter_mall where dt =date_format(date_add('DAY', -1, now()), '%Y%m%d') and env = 1 and status = 1) t1\n" +
            "    left join\n" +
            "    (select served_id,order_id,user_id,user_name from bi_dm.dm_hongjun_served_temp group by served_id,order_id,user_id,user_name) t2\n" +
            "    on t1.served_id = t2.served_id\n" +
            "    left join\n" +
            "    (select * from bi_ods.ods_sellercenter_mall_app where dt = date_format(date_add('DAY', -1, now()), '%Y%m%d') and env = 1 and status = 1) t3\n" +
            "    on t1.mall_code = t3.mall_code\n" +
            "    left join\n" +
            "    (select id,biz_type,name as app_name,name_en from bi_ods.ods_basic_app where dt = date_format(date_add('DAY', -1, now()), '%Y%m%d')) t4\n" +
            "    on cast(t3.app_id as varchar) = t4.id and cast(t3.biz_type as varchar) = t4.biz_type\n" +
            ") r2\n" +
            "\n" +
            "on r1.order_id = r2.order_id\n" +
            "left join\n" +
            "(\n" +
            "    select open_uid,name as customer_name,bd_name,dept_full_name \n" +
            "    from bi_dw.dwd_customer_item where dt = date_format(date_add('DAY', -1, now()), '%Y%m%d') and is_deleted='N'\n" +
            ") r3\n" +
            "on r2.owner_id = r3.open_uid";
    private HashSet<TableInfo> inputTables = new HashSet<>();

    @Test
    public void sparkSqlParse() throws SqlParseException {
        Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse = new SparkSQLParse().parse(sql);
        print(parse);

    }

    @Test
    public void hiveSqlParse() throws SqlParseException {
        Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse = new HiveSQLParse().parse(sql);
        print(parse);


    }

    @Test
    public void prestoSqlParse() throws SqlParseException {
        System.out.println(sql);
        Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse = new PrestoSqlParse().parse(sql);
        print(parse);
    }






}
