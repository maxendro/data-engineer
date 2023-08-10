drop table public.lab8_kuznetsov_1;

-- create table 1
CREATE TABLE public.lab8_kuznetsov_1 (
	id1  int NOT NULL,
	id2  int NOT NULL ,
	gen1 text,
	gen2 text
);


--Hint: "compresstype" is only valid for Append Only relations, create an AO relation to use "compresstype".

-- create muli-columns primary key
ALTER TABLE public.lab8_kuznetsov_1
  ADD CONSTRAINT lab8_kuznetsov_1_pk
    PRIMARY KEY (id1,id2,gen1);

--- fill data 1
insert
	into
	public.lab8_kuznetsov_1 
select
	gen,
	gen,
	gen::text || 'text1',
	gen::text || 'text2'
from
	generate_series(1,
	200000) gen;   


SELECT gp_segment_id, count(*)
FROM public.lab8_kuznetsov_1 GROUP BY gp_segment_id
ORDER BY gp_segment_id;

select
 c.oid,
 n.nspname as schemaname,
 c.relname as tablename,
 pg_get_table_distributedby(c.oid) distributedby,
 case c.relstorage
  when 'a' then ' append-optimized'
  when 'c' then 'column-oriented'
  when 'h' then 'heap'
  when 'v' then 'virtual'
  when 'x' then 'external table'
 end as "data storage mode"
from pg_class as c
inner join pg_namespace as n
 on c.relnamespace = n.oid
where
 n.nspname = 'public'
 and c.relname like '%kuznetsov%'
order by n.nspname, c.relname ;

---------------------
  
-- create table 2
CREATE TABLE public.lab8_kuznetsov_2 (
	id1  int NOT NULL,
	id2  int NOT NULL,
	gen1 text,
	gen2 text
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED by (id2);


-- fill data 2
insert
	into
	public.lab8_kuznetsov_2
select
	gen,
	gen,
	gen::text || 'text1',
	gen::text || 'text2'
from
	generate_series(1,
	200000) gen;

SELECT gp_segment_id, count(*)
FROM public.lab8_kuznetsov_2 GROUP BY gp_segment_id
ORDER BY gp_segment_id;

--------------------------------------------------

explain
select
	*
from
	public.lab8_kuznetsov_1 t1
join public.lab8_kuznetsov_2 t2 on
	t1.id1 = t2.id1
