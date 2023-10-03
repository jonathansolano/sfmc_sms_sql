select
    a.MID,
     a.KEYWORD,
     a.journey_name,
     a.journey_activity_name,
     f.fully_delivered_count as fully_delivered_ss_count,
     a.single_send_count,
     convert(Decimal(10,2),f.fully_delivered_count) / a.single_send_count as raw_fully_SMS_deliverability_Rate,
    format(convert(Decimal(10,2),f.fully_delivered_count) / a.single_send_count,'P2') as fully_SMS_deliverability_Rate,
    a.report_run_date
from ent.pod_sms_opt_out_deliverability_metrics_janame a
inner join
(select
    journeyname,
    activityname,
    '100011125' as MID,
    sharedkeyword,
    fully_delivered_count
from
(select
    journeyname,
    activityname,
    sharedkeyword,
count(Mobile) OVER(partition by JOURNEYNAME,activityname,sharedkeyword) as fully_delivered_count
from
(select
    journeyname,
    activityname,
    sharedkeyword,
    ActionDateTime,
    name,
    MessageText,
    MobileMessageTrackingID,
    mobile,
    Ordinal,
    Sent,
    Delivered,
    Undelivered,
  sum(delivered) OVER(partition by sharedkeyword, MOBILE,JOURNEYNAME,activityname,ACTIONDATETIME) as sum_delivered_check,
  max(row_number)OVER(partition by sharedkeyword, MOBILE,JOURNEYNAME,activityname,ACTIONDATETIME)as max_rownum,
  row_number,
  case when sum(delivered) OVER(partition by sharedkeyword, MOBILE,JOURNEYNAME,activityname,ACTIONDATETIME) 
  = max(row_number)OVER(partition by sharedkeyword, MOBILE,JOURNEYNAME,activityname,ACTIONDATETIME) then '1' else '0' end as fully_delivered
from
(select top 50000000000000000
    journeyname,
    activityname,
    sharedkeyword,
    ActionDateTime,
    sm.name,
    sm.MessageText,
    MobileMessageTrackingID,
    sm.mobile,
    Ordinal,
    Sent,
    Delivered,
    Undelivered,
    row_number() OVER(partition by sm.sharedkeyword, sm.MOBILE,j.JOURNEYNAME,activityname,ACTIONDATETIME order by ACTIONDATETIME desc) as row_number
from [_smsmessagetracking] sm
 inner join [_JourneyActivity] ja
 on sm.JBActivityID = ja.ActivityID
inner join [_Journey] j
on ja.VersionID = j.VersionID
where  datediff(d, ActionDateTime, getdate()) = 1
and Sent = '1' and 
SHAREDKEYWORD <> 'ATNAGIM' and (j.JourneyName not like '%uat%'
        and j.JourneyName not like '%test%') and (sm.name not like '%intro%' or ActivityName not like '%test%')

group by
    journeyname,
    activityname,
    sharedkeyword,
    sm.name,
    sm.MessageText,
    MobileMessageTrackingID,
    sm.mobile,
    Ordinal,
    ActionDateTime,
    Sent,
    Delivered,
    Undelivered
order by
    journeyname,
    activityname,
    sharedkeyword,
    sm.name,
    mobile asc) as c
group by
   journeyname,
   activityname,
    sharedkeyword,
    ActionDateTime,
    name,
    MessageText,
    MobileMessageTrackingID,
    mobile,
    Ordinal,
    Sent,
    Delivered,
    Undelivered,
    row_number) as u
where Fully_delivered = '1'
group by
    journeyname,
    activityname,
    sharedkeyword,
    mobile) as a
group by
    journeyname,
    activityname,
    sharedkeyword,
    fully_delivered_count) as f
on a.Journey_Name = f.JourneyName and a.keyword = f.sharedkeyword and a.journey_activity_name = f.ActivityName
where datediff(d, a.report_run_date, getdate()) = 0
