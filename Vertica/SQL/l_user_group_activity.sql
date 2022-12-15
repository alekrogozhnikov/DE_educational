drop table if exists ILGIBITYMYANDEXRU__DWH.l_user_group_activity;

/* Создадим таблицу l_user_group_activity*/
create table ILGIBITYMYANDEXRU__DWH.l_user_group_activity
(
	hk_l_user_group_activity bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_users REFERENCES ILGIBITYMYANDEXRU__DWH.h_users(hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_group REFERENCES ILGIBITYMYANDEXRU__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

/* Наполним таблицу l_user_group_activity*/

INSERT INTO ILGIBITYMYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select 
	DISTINCT hash(hk_user_id, hk_group_id),
	hu.hk_user_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from ILGIBITYMYANDEXRU__STAGING.group_log as gl
	left join ILGIBITYMYANDEXRU__DWH.h_users hu on hu.user_id = gl.user_id
	left join ILGIBITYMYANDEXRU__DWH.h_groups hg on hg.group_id = gl.group_id
; 