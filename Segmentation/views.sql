-- Создаем представления в схеме analysis

CREATE OR REPLACE view analysis.orderitems as
	select *
	from production.orderitems;
CREATE OR REPLACE view analysis.orders as
	select *
	from production.orders;
CREATE OR REPLACE view analysis.orderstatuses as
	select *
	from production.orderstatuses;
CREATE OR REPLACE view analysis.orderstatuslog as
	select *
	from production.orderstatuslog;
CREATE OR REPLACE view analysis.products as
	select *
	from production.products;
CREATE OR REPLACE view analysis.users as 
	select * 
	from production.users;
