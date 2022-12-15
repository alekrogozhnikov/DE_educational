Основная задача - построить аналитическое хранилище и протестировать его в деле.

Необходимый код для создания таблиц находится в папке SQL.</br>
DAGи для Apache Airflow находятся в папке dags.

Шаги для достижения поставленной цели:

1. Создаем таблицы (users, groups, dialogs, group_log) в STAGING слое для загрузки данных из источника S3</br>
    Код для создания таблиц находится в файле SQL/create_table_in_STAGING.sql

2. Используя Airflow для загрузки созданных таблиц, воспользуемся download_STAGING.py 

3. Создаем таблицы (h_users, h_groups, h_dialogs) в DWH слое для формирования аналитической схемы </br>
    Код для создания таблиц находится в файле SQL/h_tables.sql

4. Создадим и наполним таблицы связей (l_user_message, l_admins, l_groups_dialogs) </br>
    Код для создания таблиц находится в файле SQL/links_tables.sql

5. Создадим и наполним сателлиты (s_admins, s_user_chatinfo, s_user_socdem, s_group_private_status, s_admins, s_group_name и s_dialog_info) </br>
    Код находится в файле SQL/satellites.sql

6. Создадим таблицу связи l_user_group_activity и наполним ее </br>
    Код находится в файле SQL/l_user_group_activity.sql

7. Создадим и наполним сателлит s_auth_history </br>
    Код находится в файле SQL/s_auth_history.sql

8. Сформируем CTE user_group_messages </br>
    Код находится в файле SQL/CTE/user_group_messages.sql

9. Сформируем CTE user_group_log </br>
    Код находится в файле SQL/CTE/user_group_log.sql

10. SQL-запрос, который выведет по десяти самым старым группам:</br>
    - хэш-ключ группы, количество новых пользователей группы,</br>
    - количество пользователей группы, которые написали хотя бы одно сообщение,</br>
    - долю пользователей группы, которые начали общаться.</br>

    Код находится в файле SQL/CTE/return.sql