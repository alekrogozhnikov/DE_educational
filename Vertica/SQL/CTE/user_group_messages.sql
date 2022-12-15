with user_group_messages as (
		select hk_group_id, count(DISTINCT hk_user_id) cnt_users_in_group_with_messages
		from ILGIBITYMYANDEXRU__DWH.l_user_group_activity luga 
		where hk_user_id in (select hk_user_id 
							from ILGIBITYMYANDEXRU__DWH.l_user_message lum)
		group by hk_group_id
		)
select hk_group_id,
       cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;
