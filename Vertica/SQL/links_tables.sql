
/* �������� ������� ������ */


drop table if exists ILGIBITYMYANDEXRU__DWH.l_user_message;

create table ILGIBITYMYANDEXRU__DWH.l_user_message
(
hk_l_user_message bigint primary key,
hk_user_id      bigint not null CONSTRAINT fk_l_user_message_user REFERENCES ILGIBITYMYANDEXRU__DWH.h_users (hk_user_id),
hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES ILGIBITYMYANDEXRU__DWH.h_dialogs (hk_message_id),
load_dt datetime,
load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists ILGIBITYMYANDEXRU__DWH.l_admins;

create table ILGIBITYMYANDEXRU__DWH.l_admins
(
hk_l_admin_id bigint primary key,
hk_user_id      bigint not null CONSTRAINT fk_l_admins_user REFERENCES ILGIBITYMYANDEXRU__DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_admins_group REFERENCES ILGIBITYMYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists ILGIBITYMYANDEXRU__DWH.l_groups_dialogs;

create table ILGIBITYMYANDEXRU__DWH.l_groups_dialogs
(
hk_l_groups_dialogs bigint primary key,
hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_dialogs REFERENCES ILGIBITYMYANDEXRU__DWH.h_dialogs (hk_message_id),
hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES ILGIBITYMYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

/* �������� ������� ������ ������� */

INSERT INTO ILGIBITYMYANDEXRU__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
	hash(hg.hk_group_id,hu.hk_user_id),
	hg.hk_group_id,
	hu.hk_user_id,
	now() as load_dt,
	's3' as load_src
from ILGIBITYMYANDEXRU__STAGING.groups as g
	left join ILGIBITYMYANDEXRU__DWH.h_users as hu on g.admin_id = hu.user_id
	left join ILGIBITYMYANDEXRU__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from ILGIBITYMYANDEXRU__DWH.l_admins);


INSERT INTO ILGIBITYMYANDEXRU__DWH.l_user_message(hk_l_user_message, hk_user_id,hk_message_id,load_dt,load_src)
select
	hash(hu.hk_user_id,hd.hk_message_id),
	hu.hk_user_id,
	hd.hk_message_id,
	now() as load_dt,
	's3' as load_src
from ILGIBITYMYANDEXRU__STAGING.dialogs as d
	left join ILGIBITYMYANDEXRU__DWH.h_users as hu on d.message_from = hu.user_id
	left join ILGIBITYMYANDEXRU__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hash(hu.hk_user_id, hd.hk_message_id) not in (select hk_l_user_message from ILGIBITYMYANDEXRU__DWH.l_user_message);

INSERT INTO ILGIBITYMYANDEXRU__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id,hk_group_id,load_dt,load_src)
select
	hash(hd.hk_message_id,hg.hk_group_id),
	hd.hk_message_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from ILGIBITYMYANDEXRU__STAGING.groups as g
	left join ILGIBITYMYANDEXRU__DWH.h_users as hu on g.admin_id = hu.user_id
	left join ILGIBITYMYANDEXRU__DWH.h_groups as hg on g.id = hg.group_id
	left join ILGIBITYMYANDEXRU__STAGING.dialogs d on d.message_from  = hu.user_id
	left join ILGIBITYMYANDEXRU__DWH.h_dialogs hd on hd.message_id = d.message_id
where (hash(hd.hk_message_id,hg.hk_group_id) not in (select hk_l_groups_dialogs from ILGIBITYMYANDEXRU__DWH.l_groups_dialogs)) and hd.hk_message_id is not null;


