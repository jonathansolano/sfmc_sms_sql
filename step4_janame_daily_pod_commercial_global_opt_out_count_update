select
    s.MID as MID,
     s.KEYWORD as Keyword,
      s.journey_name as Journey_name,
      s.journey_activity_name,
      s.report_run_date as report_run_date,
      g.GLOBAL_OPT_OUT_COUNT as GLOBAL_OPT_OUT_COUNT
from ent.pod_sms_opt_out_deliverability_metrics_janame s
left join
(select
        z.JourneyName,
        z.activityname,
        '100011125' as MID,
        z.sharedkeyword,
        count(GLOBAL_OPT_OUT_COUNT) as GLOBAL_OPT_OUT_COUNT
from 
(select top 5000000
                j.JourneyName,
                activityname,
                sm.sharedkeyword,
                row_number() OVER(partition by sm.sharedkeyword, MOBILENumber,j.JOURNEYNAME,activityname,ACTIONDATETIME  order by ACTIONDATETIME desc) as row_number,
                sm.name,
                count(distinct MOBILENumber)  as GLOBAL_OPT_OUT_COUNT
        from [_SMSSubscriptionLog] sub
        inner join [_smsmessagetracking] sm
        on SubscriptionDefinitionID = KeywordID and Mobile = mobilenumber
        inner join [_JourneyActivity] ja
        on sm.JBActivityID = ja.ActivityID
        inner join [_Journey] j
        on ja.VersionID = j.VersionID
        where Delivered = '1' and OPTOUTSTATUSID = '1' and datediff(d, OptOutDate, getdate()) = 1
        and (j.JourneyName not like '%uat%'
        and j.JourneyName not like '%test%') and sm.name not like '%intro_%'
        group by
            j.JourneyName,
            activityname,
            sm.sharedkeyword,
                sm.name,
                ActionDateTime,
                MOBILEnumber
order by 
    j.JourneyName, activityname,sm.name asc) as z
where z.row_number = '1'
group by
     z.JourneyName,
     activityname,
        z.sharedkeyword) as g
on s.Journey_Name = g.JourneyName and s.keyword = g.sharedkeyword and s.journey_activity_name = g.ActivityName
where datediff(d, s.report_run_date, getdate()) = 0

