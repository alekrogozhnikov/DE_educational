Инстументы, обеспечивающие качество данных в источнике

| Таблица | Объект | Инстумент | Для чего используется |
| :---------------------- | :---------------------- | :---------------------- | :---------------------- |
| orders.order_id | int4 NOT NULL PRIMARY KEY | Первичный ключ | Обеспечивает уникальность записей о заказах |
| orders.order_ts | timestamp NOT NULL | Ограничение | Обеспечивает ненулевое значение записей времени о заказе |
| orders.user_id | int4 NOT NULL | Ограничение | Обеспечивает ненулевое значение записей о пользователе в заказе |
| orders.bonus_payment | numeric(19, 5) NOT NULL DEFAULT 0 | Ограничение и значение по умолчанию | Обеспечивает ненулевое значение записей и значение по умолчанию равное 0|
| orders.cost | numeric(19, 5) NOT NULL DEFAULT 0  | Ограничение, значение по умолчанию | Обеспечивает ненулевое значение записей и значение по умолчанию равное 0, записи могут быть из 19 цифр, с точносью до 5 знака|
| orders.cost | CONSTRAINT orders_check  | Ограничение | Обеспечивает равенство заказа как сумма payment + bonus_payment |
| orders.bonus_grant | numeric(19, 5) NOT NULL DEFAULT 0 | Ограничение, значение по умолчанию | Обеспечивает ненулевое значение записей и значение по умолчанию равное 0, записи могут быть из 19 цифр, с точносью до 5 знака|
| orders.status | int4 NOT NULL  | Ограничение | Обеспечивает ненулевое значение записей|
| ---------------------- | ---------------------- | ---------------------- | ------------------
| orderitems.id | int4 NOT NULL GENERATED ALWAYS AS IDENTITY  | Ограничение, автоматическая последовательная нумерация записей | Обеспечивает ненулевое уникальное значение записей |
| orderitems.product_id | int4 NOT NULL  | Ограничение | Обеспечивает ненулевое значение записей|
| orderitems.order_id | int4 NOT NULL | Ограничение | Обеспечивает ненулевое значение записей|
| orderitems.name | varchar(2048) NOT NULL | Ограничение | Обеспечивает ненулевое значение записей и ограничение в 2048 символов|
| orderitems.price | numeric(19, 5) NOT NULL DEFAULT 0 | Ограничение, значение по умолчанию | Обеспечивает ненулевое значение записей, записи могут быть из 19 цифр, с точносью до 5 знака и значение по умолчанию равное 0|
| orderitems.discount | numeric(19, 5) NOT NULL DEFAULT 0 | Ограничение, значение по умолчанию | Обеспечивает ненулевое значение записей, записи могут быть из 19 цифр, с точносью до 5 знака и значение по умолчанию равное 0|
| orderitems.quantity | int4 NOT NULL | Ограничение | Обеспечивает ненулевое значение записей|
| orderitems.discount | CONSTRAINT orderitems_check  | Ограничение | Обеспечивает discount значения >=0 и меньше или равно чем price |
| orderitems.order_id и orderitems.product_id | CONSTRAINT orderitems_order_id_product_id_key | Ограничение | Обеспечивает уникальные составные значения |
| orderitems.id | CONSTRAINT orderitems_pkey | Первичный ключ | Обеспечивает уникальность id |
| orderitems.price | CONSTRAINT orderitems_price_check | Ограничение | Обеспечивает значения больше или равным нулю |
| orderitems.quantity | CONSTRAINT orderitems_quantity_check | Ограничение | Обеспечивает значения больше или равным нулю |
| orderitems.order_id | CONSTRAINT orderitems_order_id_fkey | Внешний ключ | Обеспечивает связь с таблицей orders|
| orderitems.product_id | CONSTRAINT orderitems_product_id_fkey  | Внешний ключ | Обеспечивает связь с таблицей products|
| ---------------------- | ---------------------- | ---------------------- | ------------------ 
| orderstatuses.id | int4 NOT NULL  | Ограничение | Обеспечивает ненулевое значение записей |
| orderstatuses.key | varchar(255) NOT NULL  | Ограничение | Обеспечивает ненулевое значение записей и ограничение в 255 символов|
| orderstatuses.key | CONSTRAINT orderstatuses_pkey  | Первичный ключ | Обеспечивает уникальность id|
| ---------------------- | ---------------------- | ---------------------- | ------------------ 
| orderstatuslog.id | int4 NOT NULL GENERATED ALWAYS AS IDENTITY | Ограничение, автоматическая последовательная нумерация записей | Обеспечивает ненулевое уникальное значение записей |
| orderstatuslog.order_id | int4 NOT NULL | Ограничение | Обеспечивает ненулевое значение записей |
| orderstatuslog.status_id | int4 NOT NULL | Ограничение | Обеспечивает ненулевое значение записей |
| orderstatuslog.dttm | timestamp NOT NULL | Ограничение | Обеспечивает ненулевое значение записей |
| orderstatuslog.order_id и orderstatuslog.status_id| CONSTRAINT orderstatuslog_order_id_status_id_key | Ограничение | Обеспечивает уникальные составные значения |
| orderstatuslog.id | CONSTRAINT orderstatuslog_pkey | Первичный ключ | Обеспечивает уникальность id|
| orderstatuslog.order_id | CONSTRAINT orderstatuslog_order_id_fkey  | Внешний ключ | Обеспечивает связь с таблицей orders|
| orderstatuslog.status_id | CONSTRAINT orderstatuslog_status_id_fkey  | Внешний ключ | Обеспечивает связь с таблицей orderstatuses|
| ---------------------- | ---------------------- | ---------------------- | ------------------ 
| products.id | int4 NOT NULL | Ограничение | Обеспечивает ненулевое значение записей |
| products.name | varchar(2048) NOT NULL | Ограничение | Обеспечивает ненулевое значение записей и ограничение в 2048 символов|
| products.price | numeric(19, 5) NOT NULL DEFAULT 0 | Ограничение, значение по умолчанию | Обеспечивает ненулевое значение записей, записи могут быть из 19 цифр, с точносью до 5 знака и значение по умолчанию равное 0|
| products.id | CONSTRAINT products_pkey | Первичный ключ | Обеспечивает уникальность id|
| products.price | CONSTRAINT products_price_check | Ограничение | Обеспечивает значения больше или равным нулю |
| ---------------------- | ---------------------- | ---------------------- | ------------------ 
| users.id | int4 NOT NULL | Ограничение | Обеспечивает ненулевое значение записей |
| users.name | varchar(2048) NULL | Ограничение | Ограничение в 2048 символов и может принимать нулевые значения |
| users.login | varchar(2048) NOT NULL | Ограничение | Обеспечивает ненулевое значение записей и ограничение в 2048 символов|
| users.id | CONSTRAINT users_pkey | Первичный ключ | Обеспечивает уникальность id|





