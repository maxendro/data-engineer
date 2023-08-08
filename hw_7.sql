--drop table public.users_kuznetsov;

CREATE TABLE public.users_kuznetsov (
	user_id serial4 NOT NULL PRIMARY KEY,
	user_name varchar NOT NULL ,
	ip_address varchar NOT NULL,
	dns_name varchar NULL
)
DISTRIBUTED BY (user_id);
COMMENT ON TABLE public.users_kuznetsov IS 'информации по пользователям';

-- Column comments

COMMENT ON COLUMN public.users_kuznetsov.user_id  IS 'идентификатор';
COMMENT ON COLUMN public.users_kuznetsov.user_name IS 'имя пользователя';
COMMENT ON COLUMN public.users_kuznetsov.ip_address IS 'IP адрес пользователя';
COMMENT ON COLUMN public.users_kuznetsov.dns_name IS 'Имя сервера пользователя';

insert into public.users_kuznetsov values (1,'Kirill','10.0.77.134','www.rt.ru'),
                                          (2,'Anna','10.0.77.45','www.rt.ru')
                                          (3,'Pavel','10.0.77.134','www.gt.ru'),
                                          (4,'Olga','10.0.77.45','www.ya.ru'),
                                          (5,'Maxim','10.0.77.134','www.mgu.ru'),
                                          (6,'Ekaterina','10.0.77.45','www.nix.ru'),
                                          (7,'Anatoliy','10.0.77.134','www.reg.ru'),
                                          (8,'Irina','10.0.77.45','www.domen.ru'),
                                          (9,'Oleg','10.0.77.134','www.astra.ru'),
                                          (10,'Viktoria','10.0.77.45','www.asterisk.ru')
                                          
 -- DROP TABLE public.user_log_kuznetsov;

CREATE TABLE public.user_log_kuznetsov (
	user_id int4 NOT NULL,
	web_site varchar NULL,
	bytes int8 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib,
	compresslevel=5
)
DISTRIBUTED BY (user_id);                                    

select web_site, count(web_site) cnt_total, sum(bytes) byte_total, count(distinct user_id) uniq_usr 
FROM public.user_log_kuznetsov 
group by web_site
order by 2 desc limit 20;
