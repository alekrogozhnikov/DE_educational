CREATE table analysis.dm_rfm_segments(
	user_id int4 NOT null PRIMARY key,
	recency int2 NOT NULL CHECK(recency >= 1 AND recency <= 5),
	frequency int2 NOT NULL CHECK(recency >= 1 AND recency <= 5),
	monetary_value int2 NOT NULL CHECK(recency >= 1 AND recency <= 5)
);
