CREATE table analysis.dm_rfm_segments(
	user_id int4 NOT null PRIMARY key,
	recency int2 NOT null,
	frequency int2 NOT null,
	monetary_value int2 NOT null,
	CONSTRAINT dm_rfm_segments_recency_check CHECK((recency > (0)::numeric) and (recency <= (5)::numeric)),
	CONSTRAINT dm_rfm_segments_frequency_check CHECK((recency > (0)::numeric) and (recency <= (5)::numeric)),
	CONSTRAINT dm_rfm_segments_monetary_check CHECK((recency > (0)::numeric) and (recency <= (5)::numeric))
);
