----------------------------------------------------------
--  Given a TPC-H database schema and its content for the
--  scale factor of 0.01, we'll create and populate
--  a denormalized schema (schema no 250) in which each
-- table (normalized or denormalized) containts a single
-- JSON attribute with nested array
----------------------------------------------------------

 DROP SCHEMA IF EXISTS sf_0_01__schema_250__pure_JSON CASCADE ;
 CREATE SCHEMA sf_0_01__schema_250__pure_JSON;
-- path1: lineitem_orders_customer-nation-region
-- path2: lineitem-partsupp-part
-- path3: lineitem-partsupp_supplier_nation-region

--      Schema tables are:
--     lineitem_orders_customer
--     nation
--     region
--     lineitem
--     partsupp
--     part
--     partsupp_supplier_nation


CREATE TABLE sf_0_01__schema_250__pure_JSON.lineitem_orders_customer AS
select to_jsonb(customer_new) as customer
from
     (select customer.*,
          (select to_jsonb(array_agg(to_jsonb(orders_new)))
           from
               (select orders.*,
          (select to_jsonb(array_agg(to_jsonb(lineitem_new)))
           from
               (select lineitem.*
                from lineitem
                where lineitem.l_orderkey = orders.o_orderkey
               ) lineitem_new
         ) lineitem
                from orders
                where orders.o_custkey = customer.c_custkey
               ) orders_new
         ) orders
     from customer) customer_new WITH NO DATA ;

DO $$
  declare
    rec RECORD ;
begin
    for rec in (select * from customer) loop
     insert into sf_0_01__schema_250__pure_JSON.lineitem_orders_customer
select to_jsonb(customer_new) as customer
from
     (select customer.*,
          (select to_jsonb(array_agg(to_jsonb(orders_new)))
           from
               (select orders.*,
          (select to_jsonb(array_agg(to_jsonb(lineitem_new)))
           from
               (select lineitem.*
                from lineitem
                where lineitem.l_orderkey = orders.o_orderkey
               ) lineitem_new
         ) lineitem
                from orders
                where orders.o_custkey = customer.c_custkey
               ) orders_new
         ) orders
     from customer) customer_new
   where c_custkey = rec.c_custkey;
    end loop ;
     end ; $$
;

CREATE TABLE sf_0_01__schema_250__pure_JSON.nation AS

  SELECT to_jsonb(nation) AS nation FROM  nation;

CREATE TABLE sf_0_01__schema_250__pure_JSON.region AS

  SELECT to_jsonb(region) AS region FROM  region;

CREATE TABLE sf_0_01__schema_250__pure_JSON.lineitem AS

  SELECT to_jsonb(lineitem) AS lineitem FROM  lineitem;

CREATE TABLE sf_0_01__schema_250__pure_JSON.partsupp AS

  SELECT to_jsonb(partsupp) AS partsupp FROM  partsupp;

CREATE TABLE sf_0_01__schema_250__pure_JSON.part AS

  SELECT to_jsonb(part) AS part FROM  part;

CREATE TABLE sf_0_01__schema_250__pure_JSON.partsupp_supplier_nation AS
select to_jsonb(nation_new) as nation
from
     (select nation.*,
          (select to_jsonb(array_agg(to_jsonb(supplier_new)))
           from
               (select supplier.*,
          (select to_jsonb(array_agg(to_jsonb(partsupp_new)))
           from
               (select partsupp.*
                from partsupp
                where partsupp.ps_suppkey = supplier.s_suppkey
               ) partsupp_new
         ) partsupp
                from supplier
                where supplier.s_nationkey = nation.n_nationkey
               ) supplier_new
         ) supplier
     from nation) nation_new WITH NO DATA ;

DO $$
  declare
    rec RECORD ;
begin
    for rec in (select * from nation) loop
     insert into sf_0_01__schema_250__pure_JSON.partsupp_supplier_nation
select to_jsonb(nation_new) as nation
from
     (select nation.*,
          (select to_jsonb(array_agg(to_jsonb(supplier_new)))
           from
               (select supplier.*,
          (select to_jsonb(array_agg(to_jsonb(partsupp_new)))
           from
               (select partsupp.*
                from partsupp
                where partsupp.ps_suppkey = supplier.s_suppkey
               ) partsupp_new
         ) partsupp
                from supplier
                where supplier.s_nationkey = nation.n_nationkey
               ) supplier_new
         ) supplier
     from nation) nation_new
   where n_nationkey = rec.n_nationkey;
    end loop ;
     end ; $$
;

-- There is no PRIMARY KEY option for JSON attributes...
--...but we can use indexes...
CREATE INDEX idx__lineitem__json ON sf_0_01__schema_250__pure_JSON.lineitem USING GIN (lineitem) ;

CREATE INDEX idx__lineitem_orders_customer__json ON sf_0_01__schema_250__pure_JSON.lineitem_orders_customer USING GIN (customer) ;

CREATE INDEX idx__nation__json ON sf_0_01__schema_250__pure_JSON.nation USING GIN (nation) ;

CREATE INDEX idx__part__json ON sf_0_01__schema_250__pure_JSON.part USING GIN (part) ;

CREATE INDEX idx__partsupp__json ON sf_0_01__schema_250__pure_JSON.partsupp USING GIN (partsupp) ;

CREATE INDEX idx__partsupp_supplier_nation__json ON sf_0_01__schema_250__pure_JSON.partsupp_supplier_nation USING GIN (nation) ;

CREATE INDEX idx__region__json ON sf_0_01__schema_250__pure_JSON.region USING GIN (region) ;


-- ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ... (...)
-- NO FOREIGN KEYS AMONG JSON attributes!
