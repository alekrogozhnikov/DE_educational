
/* s_admins */

drop table if exists ILGIBITYMYANDEXRU__DWH.s_admins;

create table ILGIBITYMYANDEXRU__DWH.s_admins
(
hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES ILGIBITYMYANDEXRU__DWH.l_admins (hk_l_admin_id),
is_admin boolean,
admin_from datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO ILGIBITYMYANDEXRU__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from ILGIBITYMYANDEXRU__DWH.l_admins as la
	left join ILGIBITYMYANDEXRU__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;

/* s_group_name */

drop table if exists ILGIBITYMYANDEXRU__DWH.s_group_name;

create table ILGIBITYMYANDEXRU__DWH.s_group_name
(
hk_group_id bigint not null CONSTRAINT fk_s_group_name_l_admins REFERENCES ILGIBITYMYANDEXRU__DWH.l_admins (hk_l_admin_id),
group_name varchar(100),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO ILGIBITYMYANDEXRU__DWH.s_group_name(hk_group_id, group_name,load_dt,load_src)
select la.hk_l_admin_id,
g.group_name,
now() as load_dt,
's3' as load_src
from ILGIBITYMYANDEXRU__DWH.l_admins as la
	left join ILGIBITYMYANDEXRU__DWH.h_groups as hg on hg.hk_group_id = la.hk_group_id
	left join ILGIBITYMYANDEXRU__STAGING.groups as g on g.id = hg.group_id;

/* s_group_private_status */

drop table if exists ILGIBITYMYANDEXRU__DWH.s_group_private_status;

create table ILGIBITYMYANDEXRU__DWH.s_group_private_status
(
hk_group_id bigint not null CONSTRAINT fk_s_group_private_status_l_admins REFERENCES ILGIBITYMYANDEXRU__DWH.l_admins (hk_l_admin_id),
is_private boolean,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO ILGIBITYMYANDEXRU__DWH.s_group_private_status(hk_group_id, is_private,load_dt,load_src)
select la.hk_l_admin_id,
True as is_private,
now() as load_dt,
's3' as load_src
from ILGIBITYMYANDEXRU__DWH.l_admins as la;

/* s_dialog_info */

drop table if exists ILGIBITYMYANDEXRU__DWH.s_dialog_info;

create table ILGIBITYMYANDEXRU__DWH.s_dialog_info
(
hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_l_user_message REFERENCES ILGIBITYMYANDEXRU__DWH.l_user_message (hk_l_user_message),
message varchar(1000),
message_from int,
message_to int,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO ILGIBITYMYANDEXRU__DWH.s_dialog_info(hk_message_id, message,message_from,message_to,load_dt,load_src)
select hd.hk_message_id,
	d.message,
	d.message_from,
	d.message_to,
now() as load_dt,
's3' as load_src
from ILGIBITYMYANDEXRU__DWH.h_dialogs hd
	left join ILGIBITYMYANDEXRU__STAGING.dialogs d on d.message_id = hd.message_id;


/* s_user_socdem */

drop table if exists ILGIBITYMYANDEXRU__DWH.s_user_socdem;

create table ILGIBITYMYANDEXRU__DWH.s_user_socdem
(
hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_l_user_message REFERENCES ILGIBITYMYANDEXRU__DWH.l_user_message (hk_l_user_message),
country varchar(100),
age int,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO ILGIBITYMYANDEXRU__DWH.s_user_socdem(hk_user_id, country,age, load_dt,load_src)
select hu.hk_user_id,
u.country,
u.age,
now() as load_dt,
's3' as load_src
from ILGIBITYMYANDEXRU__DWH.h_users hu
	left join ILGIBITYMYANDEXRU__STAGING.users u on u.id = hu.user_id;

/* s_user_chatinfo */

drop table if exists ILGIBITYMYANDEXRU__DWH.s_user_chatinfo;

create table ILGIBITYMYANDEXRU__DWH.s_user_chatinfo
(
hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_l_user_message REFERENCES ILGIBITYMYANDEXRU__DWH.l_user_message (hk_l_user_message),
chat_name varchar(1000),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO ILGIBITYMYANDEXRU__DWH.s_user_chatinfo(hk_user_id, chat_name, load_dt,load_src)
select hu.hk_user_id,
u.chat_name,
now() as load_dt,
's3' as load_src
from ILGIBITYMYANDEXRU__DWH.h_users hu
	left join ILGIBITYMYANDEXRU__STAGING.users u on u.id = hu.user_id;
