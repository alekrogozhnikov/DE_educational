with user_group_messages as (
		select hk_group_id, count(DISTINCT hk_user_id) cnt_users_in_group_with_messages
		from ILGIBITYMYANDEXRU__DWH.l_user_group_activity luga 
		where hk_user_id in (select hk_user_id 
							from ILGIBITYMYANDEXRU__DWH.l_user_message lum)
		group by hk_group_id
		),
user_group_log as (
    SELECT hg.hk_group_id, count(DISTINCT hk_user_id) cnt_added_users
	from ILGIBITYMYANDEXRU__DWH.s_auth_history sah
		left join ILGIBITYMYANDEXRU__DWH.l_user_group_activity luga on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity 
		left join ILGIBITYMYANDEXRU__DWH.h_groups hg on hg.hk_group_id = luga.hk_group_id
	where event = 'add'
	group by hg.hk_group_id, hg.registration_dt
	order by hg.registration_dt desc
)
select qt.hk_group_id,
		cnt_users_in_group_with_messages,
		cnt_added_users,
		round(cnt_users_in_group_with_messages/cnt_added_users,4) group_conversion
from (select hk_group_id
		from ILGIBITYMYANDEXRU__DWH.h_groups
		order by registration_dt limit 10) qt
	left join user_group_messages ugm using(hk_group_id)
	left join user_group_log ugl using(hk_group_id)
order by group_conversion desc