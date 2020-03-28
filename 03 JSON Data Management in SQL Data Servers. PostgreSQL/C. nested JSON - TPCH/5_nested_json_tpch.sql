-- path1: lineitem_orders_customer_nation_region

drop table if exists temp1 ;
create table temp1 as
	select row_number() over () AS id, to_jsonb(x) as json_data
	from (
		select orders.*,
			(select to_jsonb(array_agg(to_jsonb(y)))
			 from (
	        	select *
    	    	from lineitem 
        		where lineitem.l_orderkey = orders.o_orderkey			 
			 ) y
			) as lineitem
		from orders ) x





drop table if exists temp1 ;
create table temp1 as
	select row_number() over () AS id, to_jsonb(x) as json_data
	from (
		select region.*,
			(select to_jsonb(array_agg(to_jsonb(y)))
			 from (
	        	select *
    	    	from nation 
        		where nation.n_regionkey = region.r_regionkey			 
			 ) y
			) as nation
		from region ) x



drop table if exists lineitem_orders_customer_nation_region_JSON_NESTED ;
create table lineitem_orders_customer_nation_region_JSON_NESTED AS
select row_number() over () AS id, to_jsonb(t) as json_data
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





select * 
from tpch0_1__2019032701024.lineitem_orders_customer_nation_region

select * 
from tpch0_1__2019032701024.lineitem_orders_customer_nation_region__JSON_FLAT




/*
 DROP SCHEMA IF EXISTS tpch0_1__2019032701024 CASCADE ; 
 CREATE SCHEMA tpch0_1__2019032701024; 
-- path1: lineitem_orders_customer_nation_region
-- conn_1_2: ~
-- path2: lineitem_partsupp_part
-- conn_2_3: ~
-- path3: lineitem_partsupp_supplier_nation_region



CREATE TABLE tpch0_1__2019032701024.lineitem_orders_customer_nation_region AS 
SELECT lineitem.l_orderkey AS l_orderkey, lineitem.l_partkey AS l_partkey, lineitem.l_suppkey AS l_suppkey, lineitem.l_linenumber AS l_linenumber, lineitem.l_quantity AS l_quantity, lineitem.l_extendedprice AS l_extendedprice, lineitem.l_discount AS l_discount, lineitem.l_tax AS l_tax, lineitem.l_returnflag AS l_returnflag, lineitem.l_linestatus AS l_linestatus, lineitem.l_shipdate AS l_shipdate, lineitem.l_commitdate AS l_commitdate, lineitem.l_receiptdate AS l_receiptdate, lineitem.l_shipinstruct AS l_shipinstruct, lineitem.l_shipmode AS l_shipmode, lineitem.l_comment AS l_comment, orders.o_orderkey AS o_orderkey, orders.o_custkey AS o_custkey, orders.o_orderstatus AS o_orderstatus, orders.o_totalprice AS o_totalprice, orders.o_orderdate AS o_orderdate, orders.o_orderpriority AS o_orderpriority, orders.o_clerk AS o_clerk, orders.o_shippriority AS o_shippriority, orders.o_comment AS o_comment, customer.c_custkey AS c_custkey, customer.c_name AS c_name, customer.c_address AS c_address, customer.c_nationkey AS c_nationkey, customer.c_phone AS c_phone, customer.c_acctbal AS c_acctbal, customer.c_mktsegment AS c_mktsegment, customer.c_comment AS c_comment, nation.n_nationkey AS n_nationkey, nation.n_name AS n_name, nation.n_regionkey AS n_regionkey, nation.n_comment AS n_comment, region.r_regionkey AS r_regionkey, region.r_name AS r_name, region.r_comment AS r_comment
FROM lineitem INNER JOIN orders ON lineitem.l_orderkey = orders.o_orderkey INNER JOIN customer ON orders.o_custkey = customer.c_custkey INNER JOIN nation ON customer.c_nationkey = nation.n_nationkey INNER JOIN region ON nation.n_regionkey = region.r_regionkey;

CREATE TABLE tpch0_1__2019032701024.lineitem_partsupp_part AS 
SELECT lineitem.l_orderkey AS l_orderkey, lineitem.l_partkey AS l_partkey, lineitem.l_suppkey AS l_suppkey, lineitem.l_linenumber AS l_linenumber, lineitem.l_quantity AS l_quantity, lineitem.l_extendedprice AS l_extendedprice, lineitem.l_discount AS l_discount, lineitem.l_tax AS l_tax, lineitem.l_returnflag AS l_returnflag, lineitem.l_linestatus AS l_linestatus, lineitem.l_shipdate AS l_shipdate, lineitem.l_commitdate AS l_commitdate, lineitem.l_receiptdate AS l_receiptdate, lineitem.l_shipinstruct AS l_shipinstruct, lineitem.l_shipmode AS l_shipmode, lineitem.l_comment AS l_comment, partsupp.ps_partkey AS ps_partkey, partsupp.ps_suppkey AS ps_suppkey, partsupp.ps_availqty AS ps_availqty, partsupp.ps_supplycost AS ps_supplycost, partsupp.ps_comment AS ps_comment, part.p_partkey AS p_partkey, part.p_name AS p_name, part.p_mfgr AS p_mfgr, part.p_brand AS p_brand, part.p_type AS p_type, part.p_size AS p_size, part.p_container AS p_container, part.p_retailprice AS p_retailprice, part.p_comment AS p_comment
FROM lineitem INNER JOIN partsupp ON lineitem.l_partkey = partsupp.ps_partkey AND lineitem.l_suppkey = partsupp.ps_suppkey INNER JOIN part ON partsupp.ps_partkey = part.p_partkey;

CREATE TABLE tpch0_1__2019032701024.lineitem_partsupp_supplier_nation_region AS 
SELECT lineitem.l_orderkey AS l_orderkey, lineitem.l_partkey AS l_partkey, lineitem.l_suppkey AS l_suppkey, lineitem.l_linenumber AS l_linenumber, lineitem.l_quantity AS l_quantity, lineitem.l_extendedprice AS l_extendedprice, lineitem.l_discount AS l_discount, lineitem.l_tax AS l_tax, lineitem.l_returnflag AS l_returnflag, lineitem.l_linestatus AS l_linestatus, lineitem.l_shipdate AS l_shipdate, lineitem.l_commitdate AS l_commitdate, lineitem.l_receiptdate AS l_receiptdate, lineitem.l_shipinstruct AS l_shipinstruct, lineitem.l_shipmode AS l_shipmode, lineitem.l_comment AS l_comment, partsupp.ps_partkey AS ps_partkey, partsupp.ps_suppkey AS ps_suppkey, partsupp.ps_availqty AS ps_availqty, partsupp.ps_supplycost AS ps_supplycost, partsupp.ps_comment AS ps_comment, supplier.s_suppkey AS s_suppkey, supplier.s_name AS s_name, supplier.s_address AS s_address, supplier.s_nationkey AS s_nationkey, supplier.s_phone AS s_phone, supplier.s_acctbal AS s_acctbal, supplier.s_comment AS s_comment, nation.n_nationkey AS n_nationkey, nation.n_name AS n_name, nation.n_regionkey AS n_regionkey, nation.n_comment AS n_comment, region.r_regionkey AS r_regionkey, region.r_name AS r_name, region.r_comment AS r_comment
FROM lineitem INNER JOIN partsupp ON lineitem.l_partkey = partsupp.ps_partkey AND lineitem.l_suppkey = partsupp.ps_suppkey INNER JOIN supplier ON partsupp.ps_suppkey = supplier.s_suppkey INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey INNER JOIN region ON nation.n_regionkey = region.r_regionkey;
ALTER TABLE tpch0_1__2019032701024.lineitem_orders_customer_nation_region ADD CONSTRAINT lineitem_orders_customer_nation_region_pkey PRIMARY KEY (l_orderkey, l_linenumber) ; 
ALTER TABLE tpch0_1__2019032701024.lineitem_partsupp_part ADD CONSTRAINT lineitem_partsupp_part_pkey PRIMARY KEY (l_orderkey, l_linenumber) ; 
ALTER TABLE tpch0_1__2019032701024.lineitem_partsupp_supplier_nation_region ADD CONSTRAINT lineitem_partsupp_supplier_nation_region_pkey PRIMARY KEY (l_orderkey, l_linenumber) ; 
ALTER TABLE tpch0_1__2019032701024.lineitem_orders_customer_nation_region ADD CONSTRAINT lineitem_orders_customer_nation_region_l_orderkey_fkey FOREIGN KEY (l_orderkey, l_linenumber)  REFERENCES tpch0_1__2019032701024.lineitem_partsupp_part (l_orderkey, l_linenumber) ; 
ALTER TABLE tpch0_1__2019032701024.lineitem_partsupp_part ADD CONSTRAINT lineitem_partsupp_part_l_orderkey_fkey FOREIGN KEY (l_orderkey, l_linenumber)  REFERENCES tpch0_1__2019032701024.lineitem_partsupp_supplier_nation_region (l_orderkey, l_linenumber) ; 

CREATE TABLE tpch0_1__2019032701024.lineitem_orders_customer_nation_region__JSON_FLAT AS SELECT row_number() over () AS id, to_jsonb(x) AS json_data FROM tpch0_1__2019032701024.lineitem_orders_customer_nation_region x ;

CREATE TABLE tpch0_1__2019032701024.lineitem_partsupp_part__JSON_FLAT AS SELECT row_number() over () AS id, to_jsonb(x) AS json_data FROM tpch0_1__2019032701024.lineitem_partsupp_part x ;

CREATE TABLE tpch0_1__2019032701024.lineitem_partsupp_supplier_nation_region__JSON_FLAT AS SELECT row_number() over () AS id, to_jsonb(x) AS json_data FROM tpch0_1__2019032701024.lineitem_partsupp_supplier_nation_region x ;

*/