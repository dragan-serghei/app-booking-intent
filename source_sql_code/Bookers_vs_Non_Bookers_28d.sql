with booking_level as (
    select *
    from (
        -- SD: Add Booking Data to the Profile & Device ID
        select
            device_id,
            install_date,
            install_timestamp,
            -- FG: to remove duplicates where a customer or device had more than 1 install in the time period. Take the earliest record --
            first_value(install_timestamp) ignore nulls over (partition by IDCUSTOMER order by install_timestamp) as first_cust_install_timestamp,
            first_value(install_timestamp) ignore nulls over (partition by DEVICE_ID order by install_timestamp) as first_device_install_timestamp,
            Install_Source,
            Install_Source_Detail,
            ip_country,
            ip_region,
            PLATFORM,
            BRAND,
            coalesce(c.idProfile, branch.profile_id) IDPROFILE,
            IDCUSTOMER,
            first_value(BOOKEDDATE) ignore nulls over (partition by IDCUSTOMER order by BOOKED) first_booked_date,
            first_value(BOOKED) ignore nulls over (partition by IDCUSTOMER order by BOOKED) first_booked,
            IDDATEFIRSTBOOKINGHW,
            IDFIRSTBOOKINGHW,
            IDBOOKING,
            BOOKED,
            BOOKEDDATE,
            ARRIVALDATE,
            DEPARTUREDATE,
            BOOKING_PLATFORM,
            case when BOOKING_PLATFORM = 'APP' and BOOKED > first_device_install_timestamp and BookedDate - Install_Date = 0 then 1 else 0 end as day0_app_bookings,
            case when BOOKING_PLATFORM = 'APP' and BOOKED > first_device_install_timestamp and BookedDate - Install_Date = 1 then 1 else 0 end as day1_app_bookings,
            case when BOOKING_PLATFORM = 'APP' and BOOKED > first_device_install_timestamp and BookedDate - Install_Date between 2 and 5 then 1 else 0 end as days2to5_app_bookings,
            case when BOOKING_PLATFORM = 'APP' and BOOKED > first_device_install_timestamp and BookedDate - Install_Date between 6 and 13 then 1 else 0 end as days6to13_app_bookings,
            case when BOOKING_PLATFORM = 'APP' and BOOKED > first_device_install_timestamp and BookedDate - Install_Date between 14 and 20 then 1 else 0 end as days14to20_app_bookings,
            case when BOOKING_PLATFORM = 'APP' and BOOKED > first_device_install_timestamp and BookedDate - Install_Date between 21 and 28 then 1 else 0 end as days21to28_app_bookings,
            case when BOOKING_PLATFORM = 'APP' and BOOKED > first_device_install_timestamp and BookedDate - Install_Date between 0 and 28 then 1 else 0 end as days0to28_app_bookings,
            case when least(IDDATEFIRSTBOOKINGHW,first_booked_date) >= date(first_device_install_timestamp) and least(IDDATEFIRSTBOOKINGHW,first_booked_date) - Install_Date = 0 then 1 else 0 end as day0_first_bkg,
            case when least(IDDATEFIRSTBOOKINGHW,first_booked_date) >= date(first_device_install_timestamp) and least(IDDATEFIRSTBOOKINGHW,first_booked_date) - Install_Date = 1 then 1 else 0 end as day1_first_bkg,
            case when least(IDDATEFIRSTBOOKINGHW,first_booked_date) >= date(first_device_install_timestamp) and least(IDDATEFIRSTBOOKINGHW,first_booked_date) - Install_Date between 2 and 5 then 1 else 0 end as day2to5_first_bkg,
            case when least(IDDATEFIRSTBOOKINGHW,first_booked_date) >= date(first_device_install_timestamp) and least(IDDATEFIRSTBOOKINGHW,first_booked_date) - Install_Date between 6 and 13 then 1 else 0 end as day6to13_first_bkg,
            case when least(IDDATEFIRSTBOOKINGHW,first_booked_date) >= date(first_device_install_timestamp) and least(IDDATEFIRSTBOOKINGHW,first_booked_date) - Install_Date between 14 and 20 then 1 else 0 end as day14to20_first_bkg,
            case when least(IDDATEFIRSTBOOKINGHW,first_booked_date) >= date(first_device_install_timestamp) and least(IDDATEFIRSTBOOKINGHW,first_booked_date) - Install_Date between 21 and 28 then 1 else 0 end as day21to28_first_bkg,
            case when Booked < first_device_install_timestamp then null else timediff(day,first_device_install_timestamp, Booked) end as Days_to_App_Booking_Post_Install,
            case    
                when least(IDDATEFIRSTBOOKINGHW,first_booked_date) < date(first_device_install_timestamp) or first_booked < first_device_install_timestamp then 'Existing Customer'
                when least(IDDATEFIRSTBOOKINGHW,first_booked_date) >= date(first_device_install_timestamp) then 'New Customer'
                when least(IDDATEFIRSTBOOKINGHW,first_booked_date) is null then 'Not a Customer'
            end as Customer_Status,
            case when first_device_install_timestamp between Booked and ArrivalDate then 1 else 0 end as Trip_Pending_YN

        from (
            -- SD: Add Profile ID to Device ID
            select 
                i.*,
                to_number(profile_id) profile_id
            from (
                -- SD: Add IP Counry/Region to each Branch Install
                select 
                    a.*,
                    case when ip_country_code = 'GB' then 'UK' else co.country end as ip_country,
                    case when ip_country = 'UK' then 'UK' else subcontinent end as ip_region
                from (
                    -- SD: Get Installs data from Branch
                    select
                        try_cast(coalesce(user_data_aaid,user_data_idfa,user_data_android_id,user_data_idfv,user_data_ip) AS string) as device_id,
                        first_value(LAST_ATTRIBUTED_TOUCH_DATA_TILDE_CAMPAIGN) ignore nulls over (partition by device_id order by timestamp_iso) as campaign, 
                        first_value(LAST_ATTRIBUTED_TOUCH_DATA_TILDE_CHANNEL) ignore nulls over (partition by device_id order by timestamp_iso) as channel,
                        first_value(LAST_ATTRIBUTED_TOUCH_DATA_TILDE_FEATURE) ignore nulls over (partition by device_id order by timestamp_iso) as feature,
                        first_value(IFNULL(case when lower(last_attributed_touch_data_tilde_advertising_partner_name) = lower('undisclosed') then 'Facebook'
                                else last_attributed_touch_data_tilde_advertising_partner_name END, 'Unpopulated')) ignore nulls over (partition by device_id order by timestamp_iso) as  Ad_Partner, 
                        first_value(LAST_ATTRIBUTED_TOUCH_DATA_DOLLAR_MARKETING_TITLE) ignore nulls over (partition by device_id order by timestamp_iso) as mktg_title,-- populated from Aug 2022 onwards,
                        first_value(date(timestamp_iso)) ignore nulls over (partition by device_id order by timestamp_iso) as install_date,
                        first_value(to_timestamp(timestamp_iso)) ignore nulls over (partition by device_id order by timestamp_iso) install_timestamp,
                        first_value(user_data_platform) ignore nulls over (partition by device_id order by timestamp_iso) platform,
                        first_value(user_data_brand) ignore nulls over (partition by device_id order by timestamp_iso) brand,
                        case when Ad_Partner = 'Google AdWords' and campaign ilike '%_rtg' then 'Google - Retargeting'
                            when Ad_Partner = 'Google AdWords' and campaign not ilike '%_rtg' then 'Google - Prospecting'
                            when Ad_Partner = 'Apple Search Ads' and campaign ilike '%brand%' then 'ASA - Brand'
                            when Ad_Partner = 'Apple Search Ads' and campaign ilike '%broad%' then 'ASA - Broad'
                            when Ad_Partner = 'Apple Search Ads' and campaign ilike '%competitors%' then 'ASA - Competitors'
                            when Ad_Partner = 'Apple Search Ads' and campaign ilike '%generics%' then 'ASA - Generics'
                            when Ad_Partner = 'Apple Search Ads' then 'ASA - Other'
                            when feature in ('paid advertising','undisclosed') and Ad_Partner <> 'Push Notifications' then concat(Ad_Partner,' - ','paid advertising')
                            when channel = 'Affiliates' then campaign
                            when channel like any ('newsletter','CRM%') or (feature = 'email' and channel is null) then 'CRM / Newsletter'
                            when feature in ('marketing','organic links','Social Media','journeys') then concat(ifnull(channel,'Other'), ' - ',feature)
                            when campaign is null and channel is null and feature is null then 'Unknown'
                            else 'Other'
                        end as Install_Source_Detail,
                        case when Ad_Partner in ('InMobi','Flymobi') then 'InMobi / Flymobi'
                            when Install_Source_Detail like 'Facebook%' then 'Facebook'
                            when Install_Source_Detail like 'Hostelworld Web%' or Install_Source_Detail in ('Blog - organic links','Desktop_Homepage_Store Badge_Android - organic links') then 'Hostelworld Web'
                            when Install_Source_Detail like 'TikTok%' then 'TikTok' 
                            when Install_Source_Detail like 'Social Hostelling%' then 'Social Hostelling Test'
                            when Ad_Partner in ('Apple Search Ads','Google AdWords') then Install_Source_Detail
                            when Ad_Partner is not null and Ad_Partner not in ('Unpopulated','Push Notifications') then Ad_Partner
                            when Install_Source_Detail in ('Unknown','CRM / Newsletter') then Install_Source_Detail
                            else 'Other' 
                        end as Install_Source,
                        first_value(USER_DATA_GEO_COUNTRY_CODE) ignore nulls over (partition by device_id order by timestamp_iso) as ip_country_code
                    from PRODUCTION.BRANCH.LOGS br
                    where date(timestamp_iso) between '2023-04-01' and '2024-04-30' -- SD: extended to 1 year so that we have more data points for the Predictive model
                        and name = 'INSTALL'
                        and user_data_platform <> 'TV_APP'
                ) as a

                left join production.reporting.lu_country as co
                on co.countryiso2 = a.ip_country_code
                left join production.reporting.lu_subcontinent as sc
                on sc.idSubContinent = co.idSubContinent
                group by all
            ) as i

            left join (
                -- SD: Get the unique Profile ID from Branch for each Device_id
                select 
                    device_id,
                    to_number(profile_id) as profile_id
                from (
                    -- SD: Get the Profile ID from Branch for each Device_id
                    -- SD: What are the rules that determine a User to get a 'Customer ID' (aka profile_id) in Branch?
                        -- It doesn't look like we're using the Device_ID anywhere except Branch, thus might be difficult to 
                        -- get insights on Non-Customers if we can't trace them as long as they don't get a Profile ID.
                    select
                        try_cast(coalesce(user_data_aaid,user_data_idfa,user_data_android_id,user_data_idfv,user_data_ip) AS string) as device_id,
                        last_value(custom_data['customer_id']) ignore nulls over (partition by device_id order by custom_data['customer_id']) as profile_id
                    -- FG: some devices have multiple profile ids
                    from PRODUCTION.BRANCH.LOGS
                    where date(timestamp_iso) between '2022-01-01' and current_date()-2 -- FG: Branch data ends June 2024
                        and length(custom_data['customer_id']) > 5
                        and custom_data['customer_id'] <> 'undefined'
                )
                group by 1,2
            ) as b
            on i.device_id = b.device_id
            where i.device_id <> '00000000-0000-0000-0000-000000000000'
            order by i.device_id, install_date
        ) as branch
        
        left join (
            -- SD: Get Bookings Data 
            select 
                b.idProfile,
                c.idCustomer,
                idDateFirstBookingHW,
                idFirstBookingHW,
                b.idBooking,
                Booked,
                BookedDate,
                ArrivalDate,
                DepartureDate,
                batt.booking_platform         
            from production.reporting.FT_Booking b
            inner join production.supply.lu_bookingattribution_localstorage as batt
                on batt.idBooking = b.idBooking
            inner join production.reporting.LU_Customer c
                on c.IdCustomer = b.IdCustomer
            inner join production.reporting.LU_Sitename sn 
                on b.idSiteName = sn.idSiteName
            inner join production.reporting.LU_Date d
                on d.idDate = c.idDateFirstBookingHW            
            where b.idBookingStatus IN (1,2,5)  
                and b.idBookingType IN (1,3) 
                and b.idSiteType <> 33
                and b.bednights > 0
                and sn.SiteBrand = 'HW'
                and batt.BOOKING_PLATFORM = 'APP'
                and bookeddate between '2023-04-01' and current_date()-2
            order by b.idBooking
        ) as c

        on c.idProfile = branch.profile_id and 
           c.booked >= branch.install_date
        order by IDCUSTOMER,
        device_id,
        idBooking
    )
),

dedups as (
    -- SD: Exclude Devices that have more than 1 customer
    select 
        device_id,
        count(distinct(IDCUSTOMER)) cust_count
    from booking_level
    group by device_id
    having cust_count <= 1
),

push_max_date as (
    -- SD: Return last Push Date if exists
    select 
        idProfile,
        max(date_push) max_date
    from PRODUCTION.APP.SOCIAL_PUSH_NOTIFICATIONS   
    group by 1
),

current_push_status as (
    -- SD: Return the status of the last Push Date if exists
    select
        p.idProfile,
        case when push_opt_in = 'TRUE' then 1 else 0 end as push_yn,
        date_push
    from PRODUCTION.APP.SOCIAL_PUSH_NOTIFICATIONS as p
    inner join push_max_date as m
        on m.idProfile = p.idProfile
           and m. max_date = p.date_push
    group by 1,2,3
),

six_mth_install_cohort as ( -- SD: named 'six' because the original data source included 6 months of data from Nov-2023 till Apr-2024
    -- SD: Aggregate data at the Device ID & Profile ID level and add Push Status
     select
        b.DEVICE_ID,
        b.idProfile,
        b.install_date,
        case 
            when dayname(b.install_date) ilike any ('%sat%', '%sun%') 
            then 1 else 0
        end as installed_on_weekend,
        b.platform,
        b.Install_Source,
        b.Install_Source_Detail,
        b.ip_country,
        b.ip_region,
        b.brand,         
        b.Customer_Status,
        max(case when b.idProfile is not null then 1 else 0 end) as profile_created_YN,
        max(ifnull(Push_YN,0)) as push_YN,
        min(first_booked_date) as first_booked_date_post_install,
        sum(days0to28_app_bookings) as days0to28_app_bookings,
    from booking_level as b
    inner join dedups as d 
        on b.device_id = d.device_id
    inner join (
        select 
            iddate,
            idyear,
            monthnameshort,
            idmonth,
            first_value(iddate) ignore nulls over (partition by idyear, idmonth order by iddate) as month_start_date,
            last_value(iddate) ignore nulls over (partition by idyear, idmonth order by iddate) as month_end_date
        from production.reporting.lu_date
        where iddate >= '2023-04-01'
    ) as me
        on me.iddate = b.install_date
    left join current_push_status as p
        on p.idProfile = b.idProfile
    where b.install_date between '2023-04-01' and '2024-04-30'
    group by all
),

previous_app_users as ( -- SD: What's the field in Firebase that we could use to link to Device ID in Branch? 
                        -- SD: this is needed to get details of App usage of Non Customers
    select
        profile_id,
        1 as previous_app_user,
        min(event_date) as earliest_app_usage
    from PRODUCTION.GOOGLE.FIREBASE_EVENTS_RAW as fb
    where fb.event_date between '2021-01-01' and '2024-04-30'
        and ((platform ilike 'ios' and (APP_INFO:version::varchar ilike any ('12%','13%'))) 
            or (platform ilike 'android' and (APP_INFO:version::varchar ilike any ('9%')))) 
        and APP_INFO:version::varchar not ilike '%staging%'
    group by all
),

app_opens as (
    select 
        try_cast(coalesce(user_data_aaid,user_data_idfa,user_data_android_id,user_data_idfv,user_data_ip) AS string) as device_id,
        date(timestamp_iso) as app_open_date,
        to_timestamp(timestamp_iso) as app_open_date_time
    from PRODUCTION.BRANCH.LOGS 
    where 
        date(timestamp_iso) between '2023-04-01' and current_date()-2 -- FG: Branch data ends June 2024
        and name = 'OPEN'
    group by all
),

app_opens_agg as (
    select 
        a.device_id,
        c.install_date,
        min(a.app_open_date) as first_app_open_date,
        datediff(day, c.install_date, first_app_open_date) as days_to_first_app_open,
        max(a.app_open_date) as last_app_open_date,
        datediff(day, c.install_date, last_app_open_date) as days_to_last_app_open,
        count(*) as app_open_count,
        count(distinct a.app_open_date) as app_open_days,
        app_open_count / nullif(app_open_days, 0) as avg_app_opens_per_day
    from app_opens as a
    join six_mth_install_cohort as c 
        on a.device_id = c.device_id
    where datediff(day, c.install_date, a.app_open_date) between 0 and 28
    group by all
),

logins as (
    select 
        try_cast(coalesce(user_data_aaid,user_data_idfa,user_data_android_id,user_data_idfv,user_data_ip) AS string) as device_id,
        date(timestamp_iso) as login_date,
        to_timestamp(timestamp_iso) as login_date_time
    from PRODUCTION.BRANCH.LOGS 
    where 
        date(timestamp_iso) between '2023-04-01' and current_date()-2 -- FG: Branch data ends June 2024
        and name = 'LOGIN'
    group by all
),

logins_agg as (
    select 
        l.device_id,
        c.install_date,
        min(l.login_date) as first_login_post_install,
        max(l.login_date) as last_login_post_install,
        count(*) as login_count,
        datediff(day, c.install_date, first_login_post_install) as days_to_first_login
    from logins as l
    join six_mth_install_cohort as c 
        on l.device_id = c.device_id
    where datediff(day, c.install_date, l.login_date) between 0 and 28
    group by all
),

pic_uploaded as (
    select 
        idProfile,
        created_at as pic_uploaded_date,
        case when "Profile Photo Flag" = 'Profiles with Photos' then 'Photo Uploaded' else 'Initials Only' end as Picture_Type
    from PRODUCTION.APP.V_SOCIAL_AUTH0
    where created_at between '2023-04-01' and current_date()-2
        and "Profile Photo Flag" = 'Profiles with Photos'
    group by all
),

social_toggle_on as (
    select
        idProfile,
        created_at as social_toggle_date,
        case when "Social Toggle Flag" = 'Social Toggle On' then 'Y' else 'N' end as Social_Member_YN
    from PRODUCTION.APP.V_SOCIAL_AUTH0
    where created_at between '2023-04-01' and current_date()-2
        and "Social Toggle Flag" = 'Social Toggle On'
    group by all
),

profile_info as (
    select 
        profile_id,
        event_date as profile_info_date,
        "Language Flag",
        "Bio Flag",
        "City Flag",
        "Interests Flag",
        "All Attributes"
    from (
        select *,
            lag(this_row) over (partition by profile_id order by event_date) as previous_row --can't create this window fn based on window fn in one query
        from (
            select 
                profile_id,
                event_date,
                max("Language Flag") over (partition by profile_id order by event_date) as "Language Flag",
                max("Bio Flag") over (partition by profile_id order by event_date) as "Bio Flag",
                max("City Flag") over (partition by profile_id order by event_date) as "City Flag",
                max("Interests Flag") over (partition by profile_id order by event_date) as "Interests Flag",
                case
                    when max("Language Flag") over (partition by profile_id order by event_date) 
                        + max("Bio Flag") over (partition by profile_id order by event_date)
                        + max("City Flag") over (partition by profile_id order by event_date)
                        + max("Interests Flag") over (partition by profile_id order by event_date) = 4 
                    then 1 else 0
                end as "All Attributes",
                concat(
                    max("Language Flag") over (partition by profile_id order by event_date), 
                    max("Bio Flag") over (partition by profile_id order by event_date),
                    max("City Flag") over (partition by profile_id order by event_date),
                    max("Interests Flag") over (partition by profile_id order by event_date)
                ) as this_row
            from PRODUCTION.RAW.SOCIAL_PROFILE_ATTRIBUTES
            order by 1,2
        )
    )
    where (this_row <> previous_row or previous_row is null) -- taking out unnecessary rows eg. city updated on a date after the original date of adding city info
        and "Language Flag" + "Bio Flag" + "City Flag" + "Interests Flag" > 0
        and event_date between '2023-04-01' and current_date()-2
    group by all
),

user_profile as (
    -- SD: I was expecting to get unique combinations of User_ID & Profile_ID but seems like we get multiple repeating rows instead. 
    -- SD: Need to re-check if this is an expected behaviour. 
    select *
    from (
        select 
            user_id,
            first_value(profile_id) ignore nulls over (partition by user_id order by sessions desc) as profile_id
        from (
            select 
                profile_id,
                coalesce(event_parameters:segment_anonymous_id::varchar,
                         user_pseudo_id,
                         coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                         profile_id,
                         user_properties :userId::varchar
                ) as user_id,
                count(distinct user_id||coalesce(event_parameters:ga_session_id::varchar,
                      user_properties:ga_session_id::varchar,
                      user_properties :first_open_time::varchar)||
                      event_date
                ) as sessions
            from production.google.firebase_events_raw as fb
            where event_date between '2023-04-01' and current_date()-2
                and platform ilike any ('ios','android') 
                and profile_id is not null
                and APP_INFO:version::varchar not ilike '%staging%'
            group by all
            order by 2,3 desc
        )
    )
    group by all
),

searches_submitted as (
    select
        search_date,
        profile_id,
        sum(searches_submitted) as searches_submitted,
        avg(lead_time) as search_avg_lead_time,
        avg(departure_date - arrival_date) as search_avg_los,
        avg(pax) as search_avg_pax,
        sum(case when search_trip_type = 'domestic' then 1 else 0 end) as search_domestic_trip,
        sum(case when search_trip_type = 'short-haul' then 1 else 0 end) as search_short_haul_trip,
        sum(case when search_trip_type = 'long-haul' then 1 else 0 end) as search_long_haul_trip
    from (
        select
            s.*,
            case
                when lower(s.destination_country) = lower(c.ip_country) then 'domestic'
                when lower(s.destination_region) = lower(c.ip_region) then 'short-haul'
                when lower(s.destination_region) != lower(c.ip_region) then 'long-haul'
                else 'not-defined'
            end as search_trip_type
        from (
            select
                event_date as search_date,
                coalesce(event_parameters:segment_anonymous_id::varchar,
                        user_pseudo_id,
                        coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                        fb.profile_id,
                        user_properties :userId::varchar
                ) as user_id,
                coalesce(fb.profile_id,u.profile_id) as profile_id, -- to try match to a profile id when search is done while not logged in
                event_parameters:arrival_date::date as arrival_date,
                event_parameters:departure_date::date as departure_date,
                event_parameters:destination_city::varchar as destination_city,
                event_parameters:destination_country::varchar as destination_country,
                sc.subcontinent as destination_region,
                event_parameters:lead_time::varchar as lead_time,
                event_parameters:number_guests::int as pax,
                1 as searches_submitted
            from production.google.firebase_events_raw as fb
            left join user_profile as u
                on u.user_id = coalesce(
                                event_parameters:segment_anonymous_id::varchar,
                                user_pseudo_id,
                                coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                                fb.profile_id,
                                user_properties :userId::varchar
                            )
            left join production.reporting.lu_country as co
                on lower(co.country) = lower(fb.event_parameters:destination_country::varchar)
            left join production.reporting.lu_subcontinent as sc
                on sc.idSubContinent = co.idSubContinent
            where event_date between '2023-04-01' and current_date()-2
                and platform ilike any ('ios','android') 
                and event_parameters:action::varchar = 'Search Submitted'
                and event_parameters:destination_country::varchar is not null
                and APP_INFO:version::varchar not ilike '%staging%'
            group by all
        ) as s
        left join six_mth_install_cohort as c
            on s.profile_id = c.idprofile
    )
    group by all
),

dstn_search_pages_viewed as (
    select
        search_date,
        profile_id,
        sum(destination_search_pages_viewed) as destination_search_pages_viewed,
        avg(properties_returned_count) as avg_number_properties_returned,
    from (
        select
            event_date as search_date,
            coalesce(event_parameters:segment_anonymous_id::varchar,
                    user_pseudo_id,
                    coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                    fb.profile_id,
                    user_properties :userId::varchar
            ) as user_id,
            coalesce(fb.profile_id,u.profile_id) as profile_id, -- to try match to a profile id when search is done while not logged in
            event_parameters:number_results::int as properties_returned_count,
            1 as destination_search_pages_viewed
        from production.google.firebase_events_raw as fb
        left join user_profile as u
            on u.user_id = coalesce(
                            event_parameters:segment_anonymous_id::varchar,
                            user_pseudo_id,
                            coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                            fb.profile_id,
                            user_properties :userId::varchar
                            )
        where event_date between '2023-04-01' and current_date()-2
            and platform ilike any ('ios','android')
            and event_name = 'Destination_Search_Page_Viewed'
            and event_parameters:page_type::varchar = 'Destination Search'
            and event_parameters:destination_country::varchar is not null
            and APP_INFO:version::varchar not ilike '%staging%'
    ) as s
    group by all
),

sort_filter_destn_prop_list as (
    select
        filter_date,
        profile_id,
        sum(filter_used) as filter_used_count,
        sum(sort_used) as sort_used_count,
    from (
        select
            event_date as filter_date,
            coalesce(event_parameters:segment_anonymous_id::varchar,
                    user_pseudo_id,
                    coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                    fb.profile_id,
                    user_properties :userId::varchar
            ) as user_id,
            coalesce(fb.profile_id,u.profile_id) as profile_id, -- to try match to a profile id when search is done while not logged in
            case when event_parameters:action::varchar like 'Filter Used' then 1 else 0 end as filter_used,
            case when event_parameters:action::varchar like 'Sort Used' then 1 else 0 end as sort_used,
        from production.google.firebase_events_raw as fb
        left join user_profile as u
            on u.user_id = coalesce(
                            event_parameters:segment_anonymous_id::varchar,
                            user_pseudo_id,
                            coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                            fb.profile_id,
                            user_properties :userId::varchar
                            )
        where event_date between '2023-04-01' and current_date()-2
            and platform ilike any ('ios','android')
            and event_name = 'Destination_Search_Page_Event'
            and event_parameters:action::varchar like any ('Filter Used', 'Sort Used')
            and event_parameters:destination_country::varchar is not null
            and APP_INFO:version::varchar not ilike '%staging%'
    ) as s
    group by all
),

recent_search_clicked as (
    -- Note: there are issues on Android with tracking, thus we get only iOS results
    -- need to check with Android team.
    select
        recent_search_date,
        profile_id,
        sum(recent_search_clicked) as recent_search_clicks
    from (
        select
            event_date as recent_search_date,
            coalesce(event_parameters:segment_anonymous_id::varchar,
                    user_pseudo_id,
                    coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                    fb.profile_id,
                    user_properties :userId::varchar
            ) as user_id,
            coalesce(fb.profile_id,u.profile_id) as profile_id, -- to try match to a profile id when search is done while not logged in
            event_parameters:number_results::int as properties_returned_count,
            1 as recent_search_clicked
        from production.google.firebase_events_raw as fb
        left join user_profile as u
            on u.user_id = coalesce(
                            event_parameters:segment_anonymous_id::varchar,
                            user_pseudo_id,
                            coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                            fb.profile_id,
                            user_properties :userId::varchar
                            )
        where event_date between '2023-04-01' and current_date()-2
            and platform ilike any ('ios','android')
            and event_name = 'Homepage_Event'
            and event_parameters:page_type::varchar = 'Homepage'
            and event_parameters:action::varchar = 'Home Panel Clicked'
            and event_parameters:panel_clicked::varchar ilike ('Searches%')  
            and APP_INFO:version::varchar not ilike '%staging%'  
    ) as s
    group by all
),

property_page_views as (
    select 
        view_date,
        profile_id,
        sum(property_page_views) as property_page_views,
        sum(case when property_type_viewed ilike '%hostel%' then 1 else 0 end) as hostels_viewed, 
        sum(case when property_type_viewed not ilike '%hostel%' then 1 else 0 end) as non_hostels_viewed, 
        avg(viewed_property_position) as viewed_property_avg_position
    from (
        select
            event_date as view_date,
            coalesce(event_parameters:segment_anonymous_id::varchar,
                    user_pseudo_id,
                    coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                    fb.profile_id,
                    user_properties:userId::varchar
            ) as user_id,
            coalesce(fb.profile_id,u.profile_id) as profile_id, -- to try match to a profile id when prop view is done while not logged in
            event_parameters:item_id::varchar as property_id_viewed,
            event_parameters:item_category::varchar as property_type_viewed,
            event_parameters:position::varchar as viewed_property_position,
            1 as property_page_views
        from production.google.firebase_events_raw as fb
        left join user_profile as u
            on u.user_id = coalesce(
                            event_parameters:segment_anonymous_id::varchar,
                            user_pseudo_id,
                            coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                            fb.profile_id,
                            user_properties :userId::varchar
                        )
        where event_date between '2023-04-01' and current_date()-2
            and platform ilike any ('ios','android')  
            and event_name ILIKE 'view_item'
            and APP_INFO:version::varchar not ilike '%staging%'
    ) 
    group by all
),

linkups_view_propt_det_page as (
   -- wihshlist feature was released end of Oct-2023
   select 
        linkup_view_date,
        profile_id,
        sum(linkups_view_propt_page) as linkups_view_propt_page
    from (
        select
            event_date as linkup_view_date,
            coalesce(event_parameters:segment_anonymous_id::varchar,
                    user_pseudo_id,
                    coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                    fb.profile_id,
                    user_properties:userId::varchar
            ) as user_id,
            coalesce(fb.profile_id,u.profile_id) as profile_id, -- to try match to a profile id when prop view is done while not logged in
            1 as linkups_view_propt_page
        from production.google.firebase_events_raw as fb
        left join user_profile as u
            on u.user_id = coalesce(
                            event_parameters:segment_anonymous_id::varchar,
                            user_pseudo_id,
                            coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                            fb.profile_id,
                            user_properties :userId::varchar
                        )
        where event_date between '2023-04-01' and current_date()-2
            and platform ilike any ('ios','android')  
            and event_parameters:page_type = 'Linkups View'
            and event_name = 'Property_Details_Linkups_View'
            and APP_INFO:version::varchar not ilike '%staging%'
    ) 
    group by all
),

checkout_starts as (
    select 
        checkout_start_date,
        profile_id,
        sum(checkout_starts) as checkout_starts
    from (
        select 
            event_date as checkout_start_date,
            coalesce(event_parameters:segment_anonymous_id::varchar,
                     user_pseudo_id,
                     coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                     fb.profile_id,
                     user_properties :userId::varchar
            ) as user_id,
            coalesce(fb.profile_id,u.profile_id) as profile_id, -- to try match to a profile id when checkout start is done while not logged in
            1 as checkout_starts
        from production.google.firebase_events_raw as fb
        left join user_profile as u
            on u.user_id = coalesce(event_parameters:segment_anonymous_id::varchar,
                            user_pseudo_id,
                            coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                            fb.profile_id,
                            user_properties :userId::varchar
                           )
        where event_date between '2023-04-01' and current_date()-2
            and platform ilike any ('ios','android') 
            and event_name ILIKE 'begin_checkout'
            and APP_INFO:version::varchar not ilike '%staging%'
    )
    group by all
),

cohort as (
    select 
        c.*,
        -- App Open info
        first_app_open_date,
        days_to_first_app_open,
        app_open_count,
        app_open_days,
        avg_app_opens_per_day,
        last_app_open_date,
        days_to_last_app_open,
        dateadd('day',28, c.install_date) - last_app_open_date as days_of_inactivity,
        -- App Login Info
        first_login_post_install,
        iff(first_login_post_install is not null, 1, 0) as did_user_ever_logged_in,
        days_to_first_login,
        login_count,
        -- Profile Info
        min(profile_info_date) as profile_info_date,
        profile_info_date - c.install_date as days_to_first_profile_info,
        max("Language Flag") as language_flag,
        max("Bio Flag") as bio_flag,
        max("City Flag") as city_flag,
        max("Interests Flag") as interest_flag,
        max("All Attributes") as all_attributes_flag,   
        min(pic_uploaded_date) as pic_uploaded_date,
        pic_uploaded_date - c.install_date as days_to_first_pic_uploaded,
        min(social_toggle_date) as social_toggle_date,
        social_toggle_date - c.install_date as days_to_first_social_toggle_on,
        Social_Member_YN
    from six_mth_install_cohort as c
    left join app_opens_agg as a
        on c.device_id = a.device_id
    left join logins_agg as l
        on c.device_id = l.device_id
    left join profile_info as o
        on c.idProfile = o.profile_id
        and o.profile_info_date - c.install_date between 0 and 28
    left join pic_uploaded as u
        on c.idProfile = u.idProfile
        and u.pic_uploaded_date - c.install_date between 0 and 28
    left join social_toggle_on as so
        on c.idProfile = so.idProfile
        and so.social_toggle_date - c.install_date between 0 and 28
    left join previous_app_users as p
        on p.profile_id = c.idProfile
    -- SD: Need to understand why are we selecting only users with 'Profile Created'?
    -- SD: and how is that assigned to a User (especially how a Non-Customer gets a Profile ID)?
    where 
        (earliest_app_usage is null or earliest_app_usage - c.install_date between 0 and 28)
        and (customer_status = 'Not a Customer' or (customer_status = 'New Customer' and days0to28_app_bookings > 0))
        and profile_created_YN = 1
    group by all
),

search_steps as (
    select
        s.profile_id,
        sum(s.search_activated) as search_activated_count,
        sum(s.started_destn_step) as started_destn_step_count,
        sum(s.invalid_destn_displayed) as invalid_destn_displayed_count,
        sum(s.destn_selected_step) as destn_selected_step_count,
        sum(s.started_dates_step) as started_dates_step_count,
        sum(s.started_guests_step) as started_guests_step_count
    from cohort as c
    inner join (
        select
            event_date as search_date,
            coalesce(fb.profile_id, u.profile_id) as profile_id, -- to try match to a profile id when search is done while not logged in
            sum(case when event_parameters:action::varchar = 'Search Activated' then 1 else 0 end) as search_activated,
            sum(case when event_parameters:action::varchar = 'Search Step' and event_parameters:step_name::varchar = 'destination' then 1 else 0 end) as started_destn_step,
            sum(case when event_parameters:action::varchar = 'Invalid Destination Displayed' then 1 else 0 end) as invalid_destn_displayed,
            sum(case when event_parameters:action::varchar = 'Destination Selected' and event_parameters:step_name::varchar = 'destination' then 1 else 0 end) as destn_selected_step,
            sum(case when event_parameters:action::varchar = 'Search Step' and event_parameters:step_name::varchar = 'dates' then 1 else 0 end) as started_dates_step,
            sum(case when event_parameters:action::varchar = 'Search Step' and event_parameters:step_name::varchar = 'guests' then 1 else 0 end) as started_guests_step
        from production.google.firebase_events_raw as fb
        left join user_profile as u
            on u.user_id = coalesce(
                            fb.event_parameters:segment_anonymous_id::varchar,
                            fb.user_pseudo_id,
                            coalesce(fb.event_parameters:ga_session_id::varchar, fb.user_properties:ga_session_id::varchar)||fb.user_properties:first_open_time::varchar, 
                            fb.profile_id,
                            fb.user_properties:userId::varchar)
        where event_date between '2023-04-01' and current_date()-2
            and platform ilike any ('ios','android') 
            and event_parameters:page_type::varchar = 'Search'
            and event_parameters:action::varchar in ('Search Activated', 'Search Step', 'Invalid Destination Displayed',
                                                     'Destination Selected')
            and APP_INFO:version::varchar not ilike '%staging%'
        group by all
    ) as s
        on c.idProfile = s.profile_id
            and s.search_date - c.install_date between 0 and 28
    group by all
),

searches_submitted_agg as (
    select
        profile_id,
        c.install_date,
        min(s.search_date) as first_search_post_install,
        datediff(day, c.install_date, first_search_post_install) as days_to_first_search_post_install,
        sum(case when search_date - c.install_date = 0 then searches_submitted else 0 end) as day0_searches_made,
        sum(case when search_date - c.install_date = 1 then searches_submitted else 0 end) as day1_searches_made,
        sum(case when (search_date - c.install_date) between 2 and 5 then searches_submitted else 0 end) as days2to5_searches_made,
        sum(case when (search_date - c.install_date) between 6 and 13 then searches_submitted else 0 end) as days6to13_searches_made,
        sum(case when (search_date - c.install_date) between 14 and 20 then searches_submitted else 0 end) as days14to20_searches_made,
        sum(case when (search_date - c.install_date) between 21 and 28 then searches_submitted else 0 end) as days21to28_searches_made,
        sum(ifnull(searches_submitted,0)) as days0to28_searches_made,
        avg(search_avg_lead_time) as search_avg_lead_time,
        avg(search_avg_los) as search_avg_los,
        avg(search_avg_pax) as search_avg_pax,
        sum(search_domestic_trip) as search_domestic_trip,
        sum(search_short_haul_trip) as search_short_haul_trip,
        sum(search_long_haul_trip) as search_long_haul_trip,
        div0(sum(search_domestic_trip), sum(searches_submitted)) as search_domestict_trip_share,
        div0(sum(search_short_haul_trip), sum(searches_submitted)) as search_short_haul_trip_share,
        div0(sum(search_long_haul_trip), sum(searches_submitted)) as search_long_haul_trip_share
    from cohort as c
    inner join searches_submitted as s
        on c.idProfile = s.profile_id
           and s.search_date - c.install_date between 0 and 28
    group by all
),

dstn_search_pages_viewed_agg as (
    select
        profile_id,
        c.install_date,
        min(s.search_date) as first_dstn_search_page_viewed_date,
        datediff(day, c.install_date, first_dstn_search_page_viewed_date) as days_to_first_dstn_search_page_viewed,
        sum(ifnull(destination_search_pages_viewed,0)) as destination_search_pages_viewed,
        avg(avg_number_properties_returned) as avg_number_properties_returned,
    from cohort as c
    inner join dstn_search_pages_viewed as s
        on c.idProfile = s.profile_id
           and s.search_date - c.install_date between 0 and 28
    group by all
),

recent_search_clicked_agg as (
    select
        rs.profile_id,
        c.install_date,
        min(rs.recent_search_date) as first_recent_search_date,
        datediff(day, c.install_date, first_recent_search_date) as days_to_first_recent_search_days,
        sum(ifnull(recent_search_clicks,0)) as recent_search_clicks
    from cohort as c
    inner join recent_search_clicked as rs
        on c.idProfile = rs.profile_id
           and rs.recent_search_date - c.install_date between 0 and 28
    group by all
),

sort_filter_destn_prop_list_agg as (
    select
        profile_id,
        min(filter_date) as first_sort_or_filter_date,
        sum(ifnull(filter_used_count,0)) as filter_used_count,
        sum(ifnull(sort_used_count,0)) as sort_used_count,
    from cohort as c
    inner join sort_filter_destn_prop_list as s
        on c.idProfile = s.profile_id
           and s.filter_date - c.install_date between 0 and 28
    group by all
),

prop_views_agg as (
    select
        profile_id,
        c.install_date,
        min(view_date) as first_prop_view_post_install_date,
        datediff(day, c.install_date, first_prop_view_post_install_date) as days_to_first_prop_view_post_install,
        sum(case when view_date - c.install_date = 0 then property_page_views else 0 end) as day0_property_views,
        sum(case when view_date - c.install_date = 1 then property_page_views else 0 end) as day1_property_views,
        sum(case when (view_date - c.install_date) between 2 and 5 then property_page_views else 0 end) as days2to5_property_views,
        sum(case when (view_date - c.install_date) between 6 and 13 then property_page_views else 0 end) as days6to13_property_views,
        sum(case when (view_date - c.install_date) between 14 and 20 then property_page_views else 0 end) as days14to20_property_views,
        sum(case when (view_date - c.install_date) between 21 and 28 then property_page_views else 0 end) as days21to28_property_views,
        sum(ifnull(property_page_views,0)) days0to28_property_views,
        sum(p.hostels_viewed) as hostels_viewed,
        sum(p.non_hostels_viewed) as non_hostels_viewed,
        div0(sum(p.hostels_viewed), sum(p.property_page_views)) as share_hostels_viewed,
        div0(sum(p.non_hostels_viewed), sum(p.property_page_views)) as share_non_hostels_viewed,
        avg(viewed_property_avg_position) as viewed_property_avg_position,    
    from cohort as c
    inner join property_page_views as p
        on c.idProfile = p.profile_id
           and p.view_date - c.install_date between 0 and 28
    group by all
),

property_page_interactions as (
   -- Except for 'Wishlist' none of these feature are available on Android; 
   -- assign 'non_available' for Android when preparing data source of ML;
   -- Have tried differnt keywords on Android to find them with no success (need to ask Android team)
   select
       i.profile_id,
       c.install_date,
        case
            when i.event_parameters:action::varchar = 'Map View Clicked'
            then i.event_date
        end as map_view_date,
        case
            when i.event_parameters:action::varchar = 'Added to Wishlist Clicked'
            then i.event_date
        end as wishlist_add_date,
        case 
            when i.event_parameters:action::varchar = 'About Read More Clicked'
            then i.event_date
        end as propt_read_more_date,
        case 
            when i.event_parameters:action::varchar = 'View All House Rules Clicked'
            then i.event_date
        end as propt_house_rules_click_date,
        case 
            when i.event_parameters:action::varchar = 'View All Facilities Clicked'
            then i.event_date
        end as propt_facilities_click_date,
        case 
            when 
                i.event_parameters:action::varchar = 'View Main Reviews Clicked' or
                i.event_parameters:action::varchar = 'View All Reviews Clicked'
            then i.event_date
        end as propt_reviews_click_date,
        case
            when i.event_parameters:action::varchar = 'Map View Clicked'
            then 1 else 0
        end as map_view_click,
        case 
            when i.event_parameters:action::varchar = 'Added to Wishlist Clicked'
            then 1 else 0
        end as wishlist_add_click,
        case 
            when i.event_parameters:action::varchar = 'About Read More Clicked'
            then 1 else 0
        end as propt_read_more_click,
        case 
            when i.event_parameters:action::varchar = 'View All House Rules Clicked'
            then 1 else 0
        end as propt_house_rules_click,
        case 
            when i.event_parameters:action::varchar = 'View All Facilities Clicked'
            then 1 else 0
        end as propt_facilities_click,
        case 
            when 
                i.event_parameters:action::varchar = 'View Main Reviews Clicked' or
                i.event_parameters:action::varchar = 'View All Reviews Clicked'
            then 1 else 0
        end as propt_reviews_click
    from (
        select
            fb.event_date,
            fb.event_parameters,
            coalesce(event_parameters:segment_anonymous_id::varchar,
                    user_pseudo_id,
                    coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                    fb.profile_id,
                    user_properties:userId::varchar
            ) as user_id,
            coalesce(fb.profile_id,u.profile_id) as profile_id, -- to try match to a profile id when prop view is done while not logged in
        from production.google.firebase_events_raw as fb
        left join user_profile as u
            on u.user_id = coalesce(
                            event_parameters:segment_anonymous_id::varchar,
                            user_pseudo_id,
                            coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                            fb.profile_id,
                            user_properties :userId::varchar
                        )
        where event_date between '2023-04-01' and current_date()-2
            and platform ilike any ('ios','android')  
            and event_name = 'Property_Details_Page_Event'
            and event_parameters:page_type::varchar = 'Property Details'
            and event_parameters:action::varchar ilike any (
                    'Map View Clicked', -- iOS only
                    'Added to Wishlist Clicked', -- feature added in Nov-2023 for both platforms
                    'About Read More Clicked', -- iOS only
                    'View All House Rules Clicked', -- iOS only
                    'View All Facilities Clicked', -- iOS only
                    'View Main Reviews Clicked', -- iOS only
                    'View All Reviews Clicked' -- iOS only
                    )
            and APP_INFO:version::varchar not ilike '%staging%'
    ) as i
    inner join cohort as c
        on c.idProfile = i.profile_id
            and i.event_date - c.install_date between 0 and 28
    group by all
),

property_page_interactions_agg as (
    select
        pi.profile_id,
        c.install_date,
        min(map_view_date) as first_prop_map_view_post_install_date,
        datediff('day', c.install_date, first_prop_map_view_post_install_date) as first_prop_map_view_post_install_days,
        sum(map_view_click) as property_page_map_clicks,
        min(wishlist_add_date) as first_whishlist_click_date,
        datediff('day', c.install_date, first_whishlist_click_date) as first_whishlist_click_days,
        sum(wishlist_add_click) as add_to_wish_list_clicks,
        min(propt_read_more_date) as first_propt_read_more_date,
        datediff('day', c.install_date, first_propt_read_more_date) as first_propt_read_more_days,
        sum(propt_read_more_click) as propt_read_more_clicks,
        min(propt_house_rules_click_date) as first_house_rules_click_date,
        datediff('day', c.install_date, first_house_rules_click_date) as first_house_rules_click_days,
        sum(propt_house_rules_click) as propt_house_rules_clicks,
        min(propt_facilities_click_date) as first_propt_facilities_click_date,
        datediff('day', c.install_date, first_propt_facilities_click_date) as first_propt_facilities_click_days,        
        sum(propt_facilities_click) as propt_facilities_clicks,

        min(propt_reviews_click_date) as first_propt_reviews_click_date,
        datediff('day', c.install_date, first_propt_reviews_click_date) as first_propt_reviews_click_days,        
        sum(propt_reviews_click) as propt_reviews_clicks,
        
    from property_page_interactions as pi
    inner join cohort as c
        on c.idProfile = pi.profile_id
    group by all
),

linkups_view_propt_det_page_agg as (
    select
        l.profile_id,
        c.install_date,
        min(linkup_view_date) as first_linkup_view_date,
        datediff('day', c.install_date, first_linkup_view_date) as first_linkup_view_days,
        sum(ifnull(linkups_view_propt_page,0)) as linkups_view_propt_page,
    from cohort as c
    inner join linkups_view_propt_det_page as l
        on c.idProfile = l.profile_id
           and l.linkup_view_date - c.install_date between 0 and 28
    group by all
),

checkout_start_agg as (
    select 
        profile_id,
        min(checkout_start_date) as first_checkout_start_post_install,
        sum(case when checkout_start_date - c.install_date = 0 then checkout_starts else 0 end) as day0_checkout_starts,
        sum(case when checkout_start_date - c.install_date = 1 then checkout_starts else 0 end) as day1_checkout_starts,
        sum(case when (checkout_start_date - c.install_date) between 2 and 5 then checkout_starts else 0 end) as days2to5_checkout_starts,
        sum(case when (checkout_start_date - c.install_date) between 6 and 13 then checkout_starts else 0 end) as days6to13_checkout_starts,
        sum(case when (checkout_start_date - c.install_date) between 14 and 20 then checkout_starts else 0 end) as days14to20_checkout_starts,
        sum(case when (checkout_start_date - c.install_date) between 21 and 28 then checkout_starts else 0 end) as days21to28_checkout_starts,
        sum(ifnull(checkout_starts,0)) days0to28_checkout_starts
    from cohort as c
    inner join checkout_starts as ch
        on c.idProfile = ch.profile_id
           and ch.checkout_start_date - c.install_date between 0 and 28
    group by all
),

checkout_steps as (
    select
        ck.profile_id,
        sum(ck.checkout_dorms_selected_count) as checkout_dorms_selected_count,
        sum(ck.checkout_privates_selected_count) as checkout_privates_selected_count,
        sum(ck.checkout_hostels_selected_count) as checkout_hostels_selected_count, 
        sum(ck.checkout_non_hostels_selected_count) as checkout_non_hostels_selected_count, 
        sum(ck.started_email_step) as started_email_step_count,
        sum(ck.started_name_step) as started_name_step_count,
        sum(ck.started_nationality_step) as started_nationality_step_count,
        sum(ck.started_payment_details_step) as started_payment_details_step_count,
        sum(ck.started_confirm_details_step) as started_confirm_details_step_count,
        sum(ck.checkout_confirm_clicked) as checkout_confirm_clicked_count
    from cohort as c
    inner join (
        select 
            event_date as checkout_date,
            coalesce(fb.profile_id,u.profile_id) as profile_id, -- to try match to a profile id when checkout is done while not logged in
            sum(case when event_name = 'begin_checkout' and event_parameters:checkout_room_types::varchar ilike ('%dorm%') then 1 else 0 end) as checkout_dorms_selected_count,
            sum(case when event_name = 'begin_checkout' and event_parameters:checkout_room_types::varchar not ilike ('%dorm%') then 1 else 0 end) as checkout_privates_selected_count,
            sum(case when event_name = 'begin_checkout' and event_parameters:item_category::varchar ilike ('%hostel%') then 1 else 0 end) as checkout_hostels_selected_count,
            sum(case when event_name = 'begin_checkout' and event_parameters:item_category::varchar not ilike ('%hostel%') then 1 else 0 end) as checkout_non_hostels_selected_count,            
            sum(case when event_parameters:step_name::varchar = 'email' then 1 else 0 end) as started_email_step,
            sum(case when event_parameters:step_name::varchar = 'full_name' then 1 else 0 end) as started_name_step,
            sum(case when event_parameters:step_name::varchar = 'nationality' then 1 else 0 end) as started_nationality_step,
            sum(case when event_parameters:step_name::varchar = 'payment_details' then 1 else 0 end) as started_payment_details_step,
            sum(case when event_parameters:step_name::varchar = 'confirm_details' then 1 else 0 end) as started_confirm_details_step,
            sum(case when event_name in ('Checkout_Confirm_Clicked') then 1 else 0 end) as checkout_confirm_clicked
        from production.google.firebase_events_raw as fb
        left join user_profile as u
            on u.user_id = coalesce( 
                            event_parameters:segment_anonymous_id::varchar,
                            user_pseudo_id,
                            coalesce(event_parameters:ga_session_id::varchar, user_properties:ga_session_id::varchar)||user_properties :first_open_time::varchar, 
                            fb.profile_id,
                            user_properties :userId::varchar)
        where event_date between '2023-04-01' and current_date()-2
            and platform ilike any ('ios','android')
            and (event_parameters:action::varchar in ('Checkout Step Viewed') or 
                 event_name in ('begin_checkout','Checkout_Confirm_Clicked'))
            and APP_INFO:version::varchar not ilike '%staging%'    
        group by all
    ) as ck
        on c.idProfile = ck.profile_id
            and ck.checkout_date - c.install_date between 0 and 28
    group by all
)

select
    c.*,
    -- search activated
    search_activated_count,
    -- search destination step
    started_destn_step_count,
    invalid_destn_displayed_count,
    destn_selected_step_count,
    search_domestic_trip,
    search_short_haul_trip,
    search_long_haul_trip,
    search_domestict_trip_share,
    search_short_haul_trip_share,
    search_long_haul_trip_share,
    -- search dates step
    started_dates_step_count,
    search_avg_lead_time,
    search_avg_los,
    -- search guests step
    started_guests_step_count,
    search_avg_pax,
    -- search submitted
    first_search_post_install,
    days_to_first_search_post_install,
    day0_searches_made,
    day1_searches_made,
    days2to5_searches_made,
    days6to13_searches_made,
    days14to20_searches_made,
    days21to28_searches_made,
    days0to28_searches_made,
    -- properties displayed
    first_dstn_search_page_viewed_date,
    days_to_first_dstn_search_page_viewed,
    destination_search_pages_viewed,
    avg_number_properties_returned,
    -- sort or filter used on displayed properties
    first_sort_or_filter_date,
    filter_used_count,
    sort_used_count,
    -- recent search clicked
    first_recent_search_date,
    days_to_first_recent_search_days,
    recent_search_clicks,
    -- properties viewed
    first_prop_view_post_install_date,
    days_to_first_prop_view_post_install,
    day0_property_views,
    day1_property_views,
    days2to5_property_views,
    days6to13_property_views,
    days14to20_property_views,
    days21to28_property_views,
    days0to28_property_views,
    hostels_viewed,
    share_hostels_viewed,
    non_hostels_viewed,
    share_non_hostels_viewed,
    viewed_property_avg_position,
    -- map click in property details page
    first_prop_map_view_post_install_date,
    first_prop_map_view_post_install_days,
    property_page_map_clicks,
    -- add to whishlist
    first_whishlist_click_date,
    first_whishlist_click_days,
    add_to_wish_list_clicks,
    -- linkup vew in property details page
    first_linkup_view_date,
    first_linkup_view_days,
    linkups_view_propt_page,
    -- Read More, Rules, Facilities
    first_propt_read_more_date,
    first_propt_read_more_days,
    propt_read_more_clicks,
    first_house_rules_click_date,
    first_house_rules_click_days,
    propt_house_rules_clicks,
    first_propt_facilities_click_date,
    first_propt_facilities_click_days,
    propt_facilities_clicks,
    -- Reviews click
    first_propt_reviews_click_date,
    first_propt_reviews_click_days,
    propt_reviews_clicks,
    -- checkout start
    first_checkout_start_post_install,
    day0_checkout_starts,
    day1_checkout_starts,
    days2to5_checkout_starts,
    days6to13_checkout_starts,
    days14to20_checkout_starts,
    days21to28_checkout_starts,
    days0to28_checkout_starts,
    -- checkout funnel
    checkout_dorms_selected_count,
    checkout_privates_selected_count,
    checkout_hostels_selected_count,
    checkout_non_hostels_selected_count,
    started_email_step_count,
    started_name_step_count,
    started_nationality_step_count,
    started_payment_details_step_count,
    started_confirm_details_step_count,
    checkout_confirm_clicked_count
from cohort as c
left join search_steps as ss
    on ss.profile_id = c.idProfile
left join searches_submitted_agg as s
    on s.profile_id = c.idProfile
left join dstn_search_pages_viewed_agg as d
    on d.profile_id = c.idProfile
left join recent_search_clicked_agg as rs
    on rs.profile_id = c.idProfile
left join sort_filter_destn_prop_list_agg as sf
    on sf.profile_id = c.idProfile
left join prop_views_agg as p
    on p.profile_id = c.idProfile
left join property_page_interactions_agg as pi
    on pi.profile_id = c.idProfile
left join linkups_view_propt_det_page_agg as lv
    on lv.profile_id = c.idProfile
left join checkout_start_agg as ch
    on ch.profile_id = c.idProfile
left join checkout_steps as cs
    on cs.profile_id = c.idProfile
where
    -- excluding users that installed the app but never opened it as there is nothing to analyze in these cases;
    -- there are instances with bookings but with app not opened, although there is activity for other events (possibly a Branch tracking issue);
    first_app_open_date is not null