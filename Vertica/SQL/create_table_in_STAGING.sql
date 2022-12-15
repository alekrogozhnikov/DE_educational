/* Создание таблицы dialogs */
drop table if exists ILGIBITYMYANDEXRU__STAGING.dialogs;
CREATE TABLE ILGIBITYMYANDEXRU__STAGING.dialogs
(
	message_id int NOT NULL,
	message_ts datetime,
	message_from int,
	message_to int,
	message varchar(1000),
	message_group int
)

/* Создание таблицы dialogs */
drop table if exists ILGIBITYMYANDEXRU__STAGING.groups;
CREATE TABLE ILGIBITYMYANDEXRU__STAGING.groups
(
    id int NOT NULL,
    admin_id int,
    group_name varchar(100),
    registration_dt timestamp,
    is_private boolean
)

/* Создание таблицы users */
drop table if exists ILGIBITYMYANDEXRU__STAGING.users;
CREATE TABLE ILGIBITYMYANDEXRU__STAGING.users
(
    id int NOT NULL,
    chat_name varchar(200),
    registration_dt timestamp,
    country varchar(200),
    age int
);

/* Создание таблицы group_log */
drop table if exists ILGIBITYMYANDEXRU__STAGING.group_log;
CREATE TABLE ILGIBITYMYANDEXRU__STAGING.group_log
(
    group_id int NOT NULL,
    user_id int NOT NULL,
    user_id_from int,
    event varchar(10) check(event = 'create' or event='add' or event = 'leave'),
    "datetime" datetime
);