-- Создаем представления в схеме analysis

CREATE OR REPLACE view orderitems as
	select *
	from production.orderitems;
CREATE OR REPLACE view orders as
	select *
	from production.orders;
CREATE OR REPLACE view orderstatuses as
	select *
	from production.orderstatuses;
CREATE OR REPLACE view orderstatuslog as
	select *
	from production.orderstatuslog;
CREATE OR REPLACE view products as
	select *
	from production.products;
