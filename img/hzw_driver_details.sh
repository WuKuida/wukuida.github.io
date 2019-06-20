#!/bin/bash
. $HOME/.bash_profile

. $HOME/.bash_profile
#########################################################################
# File Name: 司机日明细
# Author: 黄祖卫
# User: 王强
#########################################################################
# Altered Time: 2016-05-20
# Alterd By: 焦惠芸
# Alterd For: 增加1、相关消单率-----司机所有消单数中减去乘客有责、平台有责的消单数/总消单数
#2、  司机有责消单数
#3、  乘客有责消单数
#4、  司乘有责消单数
#5、  司乘无责消单数
#6、  平台责任消单数
# User: 王强
#########################################################################
# Altered Time: 2016-07-27
# Alterd By: 吴香利
# Alterd For:增加随单红包的奖励金额
#1、积分奖励总额 reward_points
#2、RMB奖励总额 reward_RMB
#########################################################################
# Altered Time: 2017-06-09
# Alterd By: 陈文雯
# Alterd For:
#将原来只统计司机状态为正常的条件修正为统计当天状态为正常、限制以及冻结状态的所有司机（状态为10,11,12）
#增加一个表示司机状态的status字段
#########################################################################
input=$1
#base_date:是指今天，因为调度系统中传入的参数是昨天
base_date=`date +%Y-%m-%d -d "1 days ${input}"`

dt=`date -d"1 day ago ${base_date}" "+%Y-%m-%d"`
dt1=`date -d"2 day ago ${base_date}" "+%Y-%m-%d"`
dt10=`date -d"10 day ago ${base_date}" "+%Y-%m-%d"`
#today1=`date "+%Y-%m-%d ${base_date}"`
today1=`date +%Y-%m-%d -d"${base_date}"`
#dt='2016-06-26'
#dt1='2016-06-25'
#today1=`date "+%Y-%m-%d"`
#today1='2016-06-27'

d1=$dt" 06:00:00"
d2=$today1" 06:00:00"
#echo "base_date:" $base_date
echo "the date of the shell is: " $dt
#echo "today is:"$today1
echo "the day before is :"$dt1

hive -e"
create table if not exists dj_test.hzw_driver_details(
dri_id                  string,
dri_name                string,
mob                     string,
company_name            string,
city_name                string,
start_work_time          string,
dri_push_cnt            string,
dri_grab_cnt            string,
dri_suc_grab_cnt        string,
du_xiang_dan_push_num   string,
gong_xiang_dan_push_num string,
shua_dan_num            string,
finish_cnt            string,
finish_pay_cnt            string,
refuse_num            string,
cancel_num            string,
online_time            string,
dri_income            string,
ord_income            string,
ord_subsidy            string,
task_reward            string,
account_check            string,
it_fee            string,
insure_fee            string,
baodan_sum            string,
baodan_suc_sum            string,
dri_cancel_ord_ratio            string,
beitoushu_sum            string,
level                     string,
mon_experiences           string,
experience                string,
score                     string,
cancel_percent string,
blame_dri_cnt int,
blame_pag_cnt int,
blame_both_cnt int,
notblame_both_cnt int,
blame_pl_cnt int,
reward_points string,
reward_RMB string
)
partitioned by (pt  string)
row format delimited fields terminated by '\t'
stored as textfile;

use dj_dw;
alter table dj_test.hzw_driver_details drop partition (pt='${dt}');
insert overwrite table dj_test.hzw_driver_details partition (pt='${dt}')
select
a5.dri_id as dri_id
,a5.dri_name as dri_name
,a5.mob as mob
,a5.company_name as company_name
,a5.city_name as city_name
,a5.start_work_time as start_work_time
,NVL(a9.dri_push_cnt,0) as dri_push_cnt
,NVL(a9.dri_grab_cnt,0) as dri_grab_cnt
,NVL(a9.dri_suc_grab_cnt,0) as dri_suc_grab_cnt
,NVL(a9.du_xiang_dan_push_num,0) as du_xiang_dan_push_num
,NVL(a9.gong_xiang_dan_push_num,0) as gong_xiang_dan_push_num
,NVL(a9.shua_dan_num,0) as shua_dan_num
,NVL(a12.finish_cnt,0) as finish_cnt
,NVL(a12.finish_pay_cnt,0) as finish_pay_cnt
,NVL(a16.refuse_num,0) as refuse_num
,NVL(a20.cancel_num,0) as cancel_num
,NVL(a21.online_time,0) as online_time
,NVL(a22.dri_income,0) as dri_income
,NVL(a22.ord_income,0) as ord_income
,NVL(a22.ord_subsidy,0) as ord_subsidy
,NVL(a22.task_reward,0) as task_reward
,NVL(a22.account_check,0) as account_check
,NVL(a22.it_fee,0) as it_fee
,NVL(a22.insure_fee,0) as insure_fee
,NVL(a25.baodan_sum,0) as baodan_sum
,NVL(a25.baodan_suc_sum,0) as baodan_suc_sum
,NVL(a28.dri_cancel_ord_ratio,0) as dri_cancel_ord_ratio
,NVL(a29.beitoushu_sum,0) as beitoushu_sum
,NVL(a30.level,0) as level
,NVL(a30.mon_experiences,0) as mon_experiences
,NVL(a31.experience,0) as experience
--,NVL(a32.score,12) as score
,NVL(a5.work_score,12) as score
,(case when x.cancel_cnt>0 then NVL(round(((x.cancel_cnt-x.blame_pag_cnt-x.blame_pl_cnt)/x.cancel_cnt),6),0) else 0 end) as cancel_percent
,NVL(x.blame_dri_cnt,0) as blame_dri_cnt
,NVL(x.blame_pag_cnt,0) as blame_pag_cnt
,NVL(x.blame_both_cnt,0) as blame_both_cnt
,NVL(x.notblame_both_cnt,0) as notblame_both_cnt
,NVL(x.blame_pl_cnt,0) as blame_pl_cnt
,red_envelope_order_daybook.reward_points as reward_points
,red_envelope_order_daybook.reward_RMB as reward_RMB
, NVL(yrf_dri_online_time_table.peak_olt_A,0) as peak_olt_A
, NVL(yrf_dri_online_time_table.peak_olt_B,0) as peak_olt_B
, NVL(yrf_ord_comment.online_moren_5star,0) as online_moren_5star
, NVL(yrf_ord_comment.online_zhudong_5star,0) as online_zhudong_5star
, NVL(yrf_ord_comment.online_4_star,0) as online_4_star
, NVL(yrf_ord_comment.online_3_star,0) as online_3_star
, NVL(yrf_ord_comment.online_2_star,0) as online_2_star
, NVL(yrf_ord_comment.online_1_star,0) as online_1_star
, NVL(yrf_ord_comment.offline_moren_5star,0) as offline_moren_5star
, NVL(yrf_ord_comment.offline_zhudong_5star,0) as offline_zhudong_5star
, NVL(yrf_ord_comment.offline_4_star,0) as offline_4_star
, NVL(yrf_ord_comment.offline_3_star,0) as offline_3_star
, NVL(yrf_ord_comment.offline_2_star,0) as offline_2_star
, NVL(yrf_ord_comment.offline_1_star,0) as offline_1_star
, NVL(yrf_dri_score_log.dri_daijia_score,0) as dri_daijia_score
, a5.status as status
, job_no

from
(
   select a2.dri_id as dri_id,a2.mob as mob,a4.name as company_name,a2.city_name as city_name,a2.name as dri_name,a2.start_work as start_work_time,a2.status as status, a2.work_score, a2.job_no
   from
   (
   select b1.dri_id as dri_id,b2.mob as mob,b2.company_id as company_id,b2.city_name as city_name,b2.name as name,b2.start_work as start_work,b1.status as status, b1.work_score, b2.job_no
   from
   (
    select distinct dri_id as dri_id,acc_status as status, work_scores as work_score
        from dwd_dri_work_info
        where pt='${dt}' and acc_status in (10,11,12)
   )b1
   join
   (
    select a1.dri_id,a1.mob,a1.company_id,a1.city_name,regexp_replace(a1.name, '\\\\n|\\\\t|\\\\r', '') name,a1.start_work,a1.job_no
    from dwd_dri_info a1
    where pt='${dt}' and start_work is not null
   )b2
   on b1.dri_id=b2.dri_id
   )a2
   left join
   (
   select a3.id,a3.name
   from ods_dj_driver_company a3
   where pt='${dt}'
   )a4
   on a2.company_id=a4.id
)a5
left outer join
(
     select
     a6.pt as pt
     ,a6.dri_id as dri_id
     ,a6.dri_push_cnt as dri_push_cnt
         ,a6.du_xiang_dan_push_num as du_xiang_dan_push_num
         ,a6.gong_xiang_dan_push_num as gong_xiang_dan_push_num
     ,a7.dri_grab_cnt as dri_grab_cnt
     ,a8.dri_suc_grab_cnt as dri_suc_grab_cnt
         ,a25.shua_dan_num as shua_dan_num
     from
     (
     select
         dri_id
         ,count(distinct ord_id) as dri_push_cnt
         ,count(distinct case when push_type=0 then ord_id end) as du_xiang_dan_push_num
         ,count(distinct case when push_type=1 then ord_id end) as gong_xiang_dan_push_num
         ,pt
     from dwd_ord_push
      where pt='${dt}'
     group by pt,dri_id
     )a6
    left join
    (
     select dri_id,count(distinct ord_id) as dri_grab_cnt,pt
     from dwd_ord_grab
     where pt='${dt}'
     group by pt,dri_id
    )a7
    on a6.dri_id=a7.dri_id
    left join
    (
    select dri_id,count(distinct ord_id) as dri_suc_grab_cnt,pt
    from dwd_ord_result
    where pt='${dt}'
    group by pt,dri_id
    )a8
    on a6.dri_id=a8.dri_id
        left outer join
        (
        select
        a23.dri_id as dri_id
        ,count(*) as shua_dan_num
        from
        (
        select ord_id,dri_id,pt
        from dwd_ord_result
        where pt='${dt}'
        )a23
        join
        (
        select *
        from dwd_ord_info
        where pt<='${dt}' and pt>='${dt1}' and risk_type=2
        )a24
        on a23.ord_id=a24.ord_id
        group by a23.dri_id,a23.pt
        )a25
     on a6.dri_id=a25.dri_id
)a9
on a5.dri_id=a9.dri_id
left outer join
(
    select
    a10.pt as pt
    ,a10.dri_id as dri_id
    ,a10.finish_cnt as finish_cnt
    ,a11.finish_pay_cnt as finish_pay_cnt
    from
    (
    select dri_id,count(distinct case when end_charge_time <>'' and end_charge_time is not  null  then ord_id end) as finish_cnt,pt
    from dwd_ord_result
    where pt='${dt}'
    group by pt,dri_id
    )a10
    left join
    (
    select dri_id,count(distinct ord_id) as finish_pay_cnt,pt
    from dwd_tra_ord_trade
    where pt='${dt}' and status in(5,7)
    group by pt,dri_id
    )a11
    on a10.dri_id=a11.dri_id
)a12
on a5.dri_id=a12.dri_id
left outer join
(
    select
    dri_id as dri_id
    ,count(distinct ord_id) as refuse_num
    from dwd_ord_refuse
    where ct_time>='${d1}' and ct_time<'${d2}' 
    and event in (1,2,5)
    and pt>='${dt10}'
    group by dri_id
)a16
on a5.dri_id=a16.dri_id
left outer join
(
    select
    a19.pt as pt
    ,a19.dri_id as dri_id
    ,count( distinct a19.ord_id) as cancel_num
    from
    (
    select
    a17.pt as pt
    ,a17.ord_id as ord_id
    ,a18.dri_id as dri_id
    from
    (
    select ord_id,pt
    from dwd_ord_info
    where pt='${dt}' and cancel_type=3
    )a17
    join
    (
    select ord_id,dri_id
    from dwd_ord_result
    where pt='${dt}'
    )a18
    on a17.ord_id=a18.ord_id
    )a19
    group by a19.pt,a19.dri_id
)a20
on a5.dri_id=a20.dri_id
left outer join
(
    select dri_id,sum(if(olt is null,0,olt)) as online_time
    from dwd_dri_online_time
    where pt='${dt}'
    group by dri_id
)a21
on a5.dri_id=a21.dri_id
left outer join
(
select dri_id,dri_income,ord_income,ord_subsidy,task_reward,account_check,it_fee,insure_fee
from dw_dri_income_day
where pt='${dt}'
)a22
on a5.dri_id=a22.dri_id
left outer join
(
select
a24.dri_id as dri_id
,count(case when a23.send_type=2 then a23.ord_id end) as baodan_sum
,count(case when a23.send_type=2 and a24.end_charge_time is not null  then a23.ord_id end) as baodan_suc_sum
from
(
select *
from dwd_ord_info
where pt='${dt}'
)a23
join
(
select *
from dwd_ord_result
where pt='${dt}'
)a24
on a23.ord_id=a24.ord_id
group by a24.dri_id
)a25
on a5.dri_id=a25.dri_id
left outer join
(
select
a26.dri_id as dri_id
,round(count(case when a27.cancel_type in (3,6) then a27.ord_id end)/count(a26.ord_id),2) as dri_cancel_ord_ratio
from
(
select *
from dwd_ord_result
where pt<='${dt}' and pt>='2015-07-28'
)a26
join
(
select *
from dwd_ord_info
where pt<='${dt}' and pt>='2015-07-27'
)a27
on a26.ord_id=a27.ord_id
group by a26.dri_id
)a28
on a5.dri_id=a28.dri_id
left outer join
(
select driver_id as dri_id,count(order_id) as beitoushu_sum
from ods_valet_order_complain
where pt='${dt}'  and complain_type='2' and handle_result='1'
group by driver_id
)a29
on a5.dri_id=a29.dri_id
left outer join
(
select driver_id,level,mon_experiences
from ods_driver_statistics
where pt='${dt}'
)a30
on a5.dri_id=a30.driver_id
left outer join
(
select driver_id,sum(value) as experience
from ods_driver_experience_record
where pt='${dt}' -- and to_date(gmt_create)='${dt}'
group by driver_id
)a31
on a5.dri_id=a31.driver_id
left outer join
(
select
driver_id,
(12+sum(cast(score as bigint))) as score
from dj_dw.ods_driver_score_log
where pt='${dt}'
group by driver_id
)a32
on a5.dri_id=a32.driver_id

left outer join
(
select
x1.dri_id
, count(distinct x1.ord_id) as cancel_cnt
, count(distinct case when x2.result='D' then x1.ord_id else null end) as blame_dri_cnt
, count(distinct case when x2.result='P' then x1.ord_id else null end) as blame_pag_cnt
, count(distinct case when x2.result='A' then x1.ord_id else null end) as blame_both_cnt
, count(distinct case when x2.result='N' then x1.ord_id else null end) as notblame_both_cnt
, count(distinct case when x2.result='PL' then x1.ord_id else null end) as blame_pl_cnt
from
(
select
dri_id
, ord_id
from dj_dw.dwd_ord_cancel
where pt='${dt}'
and dri_id is not null
) x1

left join
(
select
ord_id
, result
from dj_dw.dwd_cancel_ord_blame
where pt='${dt}'
) x2
on x1.ord_id = x2.ord_id
group by x1.dri_id
) x
on a5.dri_id=x.dri_id

left outer join
(
select
  driver_id
  ,sum(case when reward_type=2 then red_envelope_order_daybook.reward else 0 end) as reward_points
  ,sum(case when reward_type=1 then red_envelope_order_daybook.reward else 0 end) as reward_RMB
from dj_dw.dwd_ord_red_envelope_daybook red_envelope_order_daybook
where (to_date(gmt_create)='${dt}'
and substr(gmt_create,12,8)>='06:00:00')
or to_date(gmt_create)='${today1}'
and status=2
group by driver_id

)red_envelope_order_daybook
on red_envelope_order_daybook.driver_id=a5.dri_id

left join
(
    select
        dri_id
        , sum(case when segment in (46,45,44,43,42,41) then olt else 0 end) peak_olt_A
        , sum(case when segment in (48,47,46,45,44,43,42,41)  then olt else 0 end) peak_olt_B
    from
        dj_dw.dwd_dri_online_time
    where
        pt = '${dt}'
    group by dri_id
) yrf_dri_online_time_table
on a5.dri_id = yrf_dri_online_time_table.dri_id
left join
(

    select
        dri_id
        --乘客发单、400、和未匹配到订单的算线上
        , sum(case when (yrf_t2.send_type in (1,3) or yrf_t2.send_type is null) and comment_star = 5 and memo = '系统默认好评' then 1 else 0 end)  as online_moren_5star
        , sum(case when (yrf_t2.send_type in (1,3) or yrf_t2.send_type is null) and comment_star = 5 and memo <> '系统默认好评' then 1 else 0 end) as online_zhudong_5star
        , sum(case when (yrf_t2.send_type in (1,3) or yrf_t2.send_type is null) and comment_star = 4 then 1 else 0 end) as online_4_star
        , sum(case when (yrf_t2.send_type in (1,3) or yrf_t2.send_type is null) and comment_star = 3 then 1 else 0 end) as online_3_star
        , sum(case when (yrf_t2.send_type in (1,3) or yrf_t2.send_type is null) and comment_star = 2 then 1 else 0 end) as online_2_star
        , sum(case when (yrf_t2.send_type in (1,3) or yrf_t2.send_type is null) and comment_star = 1 then 1 else 0 end) as online_1_star

        , sum(case when yrf_t2.send_type = 2 and comment_star = 5 and memo = '系统默认好评' then 1 else 0 end)  as offline_moren_5star
        , sum(case when yrf_t2.send_type = 2 and comment_star = 5 and memo <> '系统默认好评' then 1 else 0 end) as offline_zhudong_5star
        , sum(case when yrf_t2.send_type = 2 and comment_star = 4 then 1 else 0 end) as offline_4_star
        , sum(case when yrf_t2.send_type = 2 and comment_star = 3 then 1 else 0 end) as offline_3_star
        , sum(case when yrf_t2.send_type = 2 and comment_star = 2 then 1 else 0 end) as offline_2_star
        , sum(case when yrf_t2.send_type = 2 and comment_star = 1 then 1 else 0 end) as offline_1_star
    from
    (
    select
        *
    from
         dj_dw.dwd_ord_comment
    where
        pt = '${dt}'
    ) yrf_t1
    left join
    (
    select
        ord_id
        , send_type
    from
        dj_dw.dwd_ord_info
    where
        --因为系统默认好评是支付后72小时，往前取三个月的订单尝试关联
        pt >= date_add('${dt}',-90)
    )  yrf_t2
    on yrf_t1.ord_id = yrf_t2.ord_id
    group by yrf_t1.dri_id
) yrf_ord_comment
on a5.dri_id = yrf_ord_comment.dri_id
left join
(
    select
        dri_id
        , sum(score) as dri_daijia_score
    from
        dj_dw.dwd_dri_score_log
    where
        pt = '${dt}'
    group by dri_id
) yrf_dri_score_log
on a5.dri_id = yrf_dri_score_log.dri_id




;

"

hadoop fs -touchz /user/dj_bi/warehouse/dj_test.db/hzw_driver_details/pt=${dt}/_SUCCESS

#echo "$dt，司机明细表" | python /data/work/mail/dpm_sendmail "司机明细表" 'pengyongfei@didichuxing.com','chenyijing@didichuxing.com','sunjianxiong@didichuxing.com','hanyuehong@didichuxing.com','luying@didichuxing.com','wangqiang@didichuxing.com','guanliwei@didichuxing.com','wupeiyi@didichuxing.com' /data/work/huangzuwei/chenyijing/siji_detail_$dt.txt



