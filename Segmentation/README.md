Задача состоит в сегментации клиентов, при котором анализируют их лояльность: как часто, на какие суммы и когда в последний раз тот или иной клиент покупал что-то. Построить витрину на основе данной сегментации.

[comments.txt](comments.txt) - содержит переписку с ревьюером;

[datamart_ddl.sql](datamart_ddl.sql) - содержит код создания ветрины с необходимыми ограничениями;

[requirements.md](requirements.md) - описывает ход рассуждений для решения задачи;

[views.sql](views.sql) - содержит код создания представлений;

[data_quality.md](data_quality.md) - содержится описание исходных данных;

[tmp_rfm_recency.sql](tmp_rfm_recency.sql) - SQL-запрос для заполнения таблицы analysis.tmp_rfm_recency

[tmp_rfm_frequency.sql](tmp_rfm_frequency.sql) - SQL-запрос для заполнения таблицы analysis.tmp_rfm_frequency

[tmp_rfm_monetary_value.sql](tmp_rfm_monetary_value.sql) - SQL-запрос для заполнения таблицы analysis.tmp_rfm_monetary_value

[datamart_query.sql](datamart_query.sql) - SQL-запрос для заполнения витрины dm_rfm_segments

[orders_view.sql](orders_view.sql) - SQL-код, который обновит представление и добавит поле status для построения ветрины
