select
    s.MID as MID,
     s.KEYWORD as Keyword,
      s.journey_name as Journey_name,
      s.journey_activity_name,
      s.report_run_date as report_run_date,
      ss.single_send_count as single_send_count
from
/* The below subquery can be used within any data extension to find sms sent no matter the length of the SMS sent.*/
(Select
    x.journeyname,
    x.ActivityName,
    x.sharedkeyword,
    count(x.mobile) as single_send_count
from (select top 50000000000000000
    journeyname,
    ActivityName,
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
    row_number() OVER(partition by sm.sharedkeyword, sm.MOBILE,j.JOURNEYNAME,ActivityName, ACTIONDATETIME  order by ACTIONDATETIME desc) as row_number
from [_smsmessagetracking] sm
 inner join [_JourneyActivity] ja
 on sm.JBActivityID = ja.ActivityID
inner join [_Journey] j
on ja.VersionID = j.VersionID
where  datediff(d, ActionDateTime, getdate()) = 1
and Sent = '1' and 
(SHAREDKEYWORD <> 'ATNAGIM'  or SHAREDKEYWORD <> 'ATNAGIMMDCR') and (j.JourneyName not like '%uat%'
        and j.JourneyName not like '%test%') and (sm.name not like '%intro%' 
        or ActivityName not like '%test%' or ActivityName not like '%intro%')
group by
    journeyname,
    ActivityName,
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
    ActivityName,
    sharedkeyword asc) as x
where x.row_number = 1
group by
    x.journeyname,
    x.ActivityName,
    x.sharedkeyword) as ss
inner join ent.pod_sms_opt_out_deliverability_metrics_janame s
on s.Journey_Name = ss.JourneyName and s.keyword = ss.sharedkeyword and s.journey_activity_name = ss.ActivityName
where datediff(d, s.report_run_date, getdate()) = 0
