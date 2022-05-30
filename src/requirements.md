<h1 align="center">ДЕКОМПОЗИЦИЯ</h1>

1. Для расчета метрик и построения ветрины ``dm_rfm_segments`` будет использоваться таблица ``orders`` и ее данные:``order_id``,  ``order_ts``, ``user_id``, ``cost``.</br>
для метрики recency используется ``orders.user_id`` и ``orders.order_ts``; </br>
для метрики frequency используется ``orders.user_id`` и ``orders.order_id``; </br>
для метрики monetary_value используется ``orders.user_id`` и ``orders.cost``; </br>

