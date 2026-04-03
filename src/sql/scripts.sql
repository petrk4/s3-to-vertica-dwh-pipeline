--создание stg слоя
CREATE TABLE VT260322CCD83B__STAGING.group_log (
	group_id int REFERENCES VT260322CCD83B__STAGING.groups (id),
	user_id int REFERENCES VT260322CCD83B__STAGING.users (id) ,
	user_id_from int REFERENCES VT260322CCD83B__STAGING.users (id),
	event varchar(50),
	datetime timestamp
)
ORDER BY group_id
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);

--создание линка
create table VT260322CCD83B__DWH.l_user_group_activity
(
	hk_l_user_group_activity int primary key,
	hk_user_id int not null CONSTRAINT fk_l_user_group_activity_user REFERENCES VT260322CCD83B__DWH.h_users (hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_group REFERENCES VT260322CCD83B__DWH.h_groups  (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--наполнение линка
INSERT INTO VT260322CCD83B__DWH.l_user_group_activity(hk_l_user_group_activity,hk_user_id,hk_group_id,load_dt,load_src)
select
	hash(hg.hk_group_id,hu.hk_user_id),
	hu.hk_user_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from VT260322CCD83B__STAGING.group_log gl
left join VT260322CCD83B__DWH.h_users as hu on gl.user_id  = hu.user_id
left join VT260322CCD83B__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from VT260322CCD83B__DWH.l_admins);

--создание саттелита
create table VT260322CCD83B__DWH.s_auth_history
(
	hk_l_user_group_activity int not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES VT260322CCD83B__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar(20),
	event_dt TIMESTAMP ,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--наполнение саттелита
INSERT INTO VT260322CCD83B__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
select
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.datetime,
	now() as load_dt,
	's3' as load_src
from VT260322CCD83B__STAGING.group_log as gl
left join VT260322CCD83B__DWH.h_groups as hg on gl.group_id = hg.group_id
left join VT260322CCD83B__DWH.h_users as hu on gl.user_id = hu.user_id
RIGHT join VT260322CCD83B__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
;

--итоговый запрос
with user_group_messages as (
    select
    	hk_group_id,
    	count(DISTINCT lum.hk_user_id) cnt_users_in_group_with_messages
	FROM VT260322CCD83B__DWH.l_groups_dialogs lgd
	LEFT JOIN VT260322CCD83B__DWH.l_user_message lum ON lgd.hk_message_id = lum.hk_message_id
	GROUP BY hk_group_id
),
top_groups AS (
    SELECT hg.hk_group_id
    FROM VT260322CCD83B__DWH.h_groups hg
    ORDER BY hg.registration_dt
    LIMIT 10
),
add_events AS (
    SELECT DISTINCT hk_l_user_group_activity
    FROM VT260322CCD83B__DWH.s_auth_history
    WHERE event = 'add'
),
user_group_log AS (
	SELECT
	    gr.hk_group_id,
	    COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
	FROM top_groups gr
	LEFT JOIN VT260322CCD83B__DWH.l_user_group_activity luga
	    ON luga.hk_group_id = gr.hk_group_id
	LEFT JOIN add_events sah
	    ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
	GROUP BY gr.hk_group_id
	ORDER BY cnt_added_users
	LIMIT 10
)
SELECT
	user_group_log.hk_group_id,
	cnt_added_users,
	cnt_users_in_group_with_messages,
	cnt_users_in_group_with_messages / cnt_added_users AS group_conversion
FROM user_group_log
LEFT JOIN user_group_messages ON user_group_log.hk_group_id = user_group_messages.hk_group_id
order by cnt_users_in_group_with_messages / cnt_added_users desc
;