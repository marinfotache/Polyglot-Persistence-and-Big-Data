select *
from tpch0_1__2019032700001.region


drop table if exists temp ;
create table temp as 
	select 1 as id, to_jsonb(r) 
	from tpch0_1__2019032700001.region r;

select * from temp

drop table if exists temp1 ;
create table temp1 AS
select t.r_regionkey, to_jsonb(t) as json_data
from (
  select r.*,
    (
      select to_jsonb(array_agg(to_jsonb(x)))
      from (
        select *
        from tpch0_1__2019032700001.nation n
        where n.n_regionkey = r.r_regionkey
      ) x
    ) as nation
  from tpch0_1__2019032700001.region r
) t ;

select * from temp1 ;


--
-- join `customers` with `temp1` - v1 ()
with t1 as 
	(SELECT r_regionkey,
		json_data ->> 'r_name' AS r_name,
		json_data ->> 'r_comment' AS r_comment,
		json_data ->> 'nation' AS nation,	 
		CAST (jsonb_array_elements(json_data -> 'nation')->> 'n_nationkey' AS integer) as n_nationkey,	 
		jsonb_array_elements(json_data -> 'nation')->> 'n_name' as n_name,	 
		CAST(jsonb_array_elements(json_data -> 'nation')->> 'n_regionkey' AS integer) as n_regionkey	 	 
	FROM temp1)
select * 
from t1 INNER JOIN customer ON t1.n_nationkey = customer.c_nationkey
		
		
drop table if exists customer_json_flat ;
create table customer_json_flat as 
	select row_number() over () as id,
	to_jsonb(c) as json_data
	from tpch0_1__2019032700001.customer c;

select * from customer_json_flat

select row_number() over () from customer



