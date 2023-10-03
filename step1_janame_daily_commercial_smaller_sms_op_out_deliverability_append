select
    'Commercial' as SFMC_BU,
    '100011125' as MID,
    s.JourneyName as journey_name,
    s.ActivityName as journey_activity_name,
    s.sharedkeyword as keyword,
    d.delivered_count,
    s.send_count,
    format(convert(Decimal(10,2),d.delivered_count) / s.send_count,'P2') as SMS_deliverability_rate,
    convert(Decimal(10,2),d.delivered_count) / s.send_count as raw_SMS_deliverability_rate,
    getdate() as report_run_date
from (Select
    x.journeyname,
    x.ActivityName,
    x.sharedkeyword,
    count(x.mobile) as send_count
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
    row_number() OVER(partition by sm.sharedkeyword, sm.MOBILE,j.JOURNEYNAME, ActivityName,ACTIONDATETIME  order by ACTIONDATETIME desc) as row_number
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
    sharedkeyword,
    sm.name,
    mobile asc) as x
group by
    x.journeyname,
    x.ActivityName,
    x.sharedkeyword) as s
    
inner join
(Select
    x.journeyname,
    x.ActivityName,
    x.sharedkeyword,
    count(x.mobile) as delivered_count
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
    row_number() OVER(partition by sm.sharedkeyword, sm.MOBILE,j.JOURNEYNAME, ActivityName, ACTIONDATETIME  order by ACTIONDATETIME desc) as row_number
from [_smsmessagetracking] sm
 inner join [_JourneyActivity] ja
 on sm.JBActivityID = ja.ActivityID
inner join [_Journey] j
on ja.VersionID = j.VersionID
where  datediff(d, ActionDateTime, getdate()) = 1
and Sent = '1' and Delivered = '1' and
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
    sharedkeyword,
    sm.name,
    mobile asc) as x
group by
    x.journeyname,
    x.ActivityName,
    x.sharedkeyword) as d
on s.JourneyName = d.JourneyName and s.sharedkeyword = d.sharedkeyword and s.ActivityName = d.ActivityName

    
group by
    s.JourneyName,
    s.ActivityName,
    s.sharedkeyword,
    s.send_count,
    d.delivered_count
