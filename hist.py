# coding: utf-8

import pymysql
import sys
import datetime
from odps import ODPS

reload(sys)
sys.setdefaultencoding('utf-8')

access_id = 'vXoVjvIHmhjQTzq2'
access_key = 'kXRoJ8sbY5sGH15uCgUmLj3Rm2mAFk'
project = 'test_dw_hz'
ep = 'http://service.cn-huzhou-hzdsj-d01.odps.ops.cloud.huzhou.gov.cn/api'
o = ODPS(access_id, access_key, project, endpoint=ep)

# hist_create() 拉链表sql处理过程
# 输入参数说明：
# tb_name：源头表名
# tb_hist：结果拉链表名
# col_key：主建字段名
# col_time：时间字段名
def hist_create(tb_name,tb_hist,col_key,col_time):

    table = o.get_table(tb_name)
    col_list = []
    col_value_list = []
    temp_tb_01 = tb_name + '001'
    temp_tb_02 = tb_name + '002'
    temp_tb_03 = tb_name + '003'

    for col in table.schema.columns:
        col_list.append(col.name)

    for col in table.schema.columns:
        if col.name not in [col_key,col_time]:
            col_value_list.append(col.name)

    cols = ','.join(col_list)
    cols_value = ','.join(col_value_list)

    create_tab_01 = 'CREATE TABLE IF NOT EXISTS ' + temp_tb_01 +  ' AS ' \
                    'select ' + cols + ' ' \
                    'from ( ' \
                        'select ' + cols + ' ' \
                        'from ' + tb_name + ' ' \
                        'union all ' \
                        'select ' + cols + ' ' \
                        'from ' + tb_hist + ' where is_current = "1" )t ' \
                    'group by ' + cols + ';'

    create_tab_02 = 'create table if not exists '+ temp_tb_02 +' as ' \
                    ''
    print create_tab_01

    o.execute_sql(create_tab_01)

    create_tab_02 = 'create table if not exists ' + temp_tb_02 + ' as ' \
                    'select ' + col_key + ',' + cols_value + ',' + 'min('+col_time+') as ' + col_time + ',row_num1 - row_num2 AS row_num from(' \
                        'select ' + cols + ',' \
                            'row_number() over(partition by '+col_key+' order by '+col_time+') as row_num1,' \
                            'row_number() over(partition by '+col_key+','+cols_value+' order by '+col_time+') as row_num2 ' \
                        'from '+ temp_tb_01 + ') t ' \
                    'group by '+col_key+','+cols_value + ',row_num1 - row_num2;'

    print create_tab_02

    o.execute_sql(create_tab_02)

    create_tab_03 = 'create table if not exists '+temp_tb_03+' as '\
                     'select '+ cols + ','+ col_time +' as eff_date,lead('+col_time+') over (partition by '+col_key+' order by '+ \
                    col_time+') as inv_date from '+ temp_tb_02 + ';'

    print create_tab_03

    o.execute_sql(create_tab_03)

    create_tab_04 = 'INSERT OVERWRITE TABLE '+tb_hist+' select '+ cols +',eff_date,inv_date,is_current'\
  ' from (select '+ cols +',eff_date,inv_date,case when inv_date is null then "1" else "0" end as is_current'\
  ' from '+ temp_tb_03 + ' ' \
                    ' union all ' \
                    'select '+ cols +',eff_date,inv_date,is_current ' \
                    'from ' + tb_hist + ' where is_current="0" ' \
                    ')t group by '+ cols +',eff_date,inv_date,is_current'

    print create_tab_04

    o.execute_sql(create_tab_04)

    delete_tab(temp_tb_01)
    delete_tab(temp_tb_02)
    delete_tab(temp_tb_03)

# delete_tab() 删除表
# 输出参数说明：
# tb_name：需要删除的表名
def delete_tab(tb_name):
    o.execute_sql('drop table ' + tb_name)

if __name__ == "__main__":

    # 调用hist_create()函数，输入参数
    hist_create('test_hist_source','test_hist','p_name','p_date')

    print '拉链表完成'