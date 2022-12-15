with user_group_log as (
    SELECT hg.hk_group_id, count(DISTINCT hk_user_id) cnt_added_users
	from ILGIBITYMYANDEXRU__DWH.s_auth_history sah
		left join ILGIBITYMYANDEXRU__DWH.l_user_group_activity luga on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity 
		left join ILGIBITYMYANDEXRU__DWH.h_groups hg on hg.hk_group_id = luga.hk_group_id
	where event = 'add'
	group by hg.hk_group_id, hg.registration_dt
	order by hg.registration_dt desc
)
select hk_group_id,
		cnt_added_users
from user_group_log
order by cnt_added_users
limit 10
;





