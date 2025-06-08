create table users_2(
	id serial primary key,
	name varchar(25),
	age smallint not null check (age >= 0),
	gender boolean not null default true,
	salary numeric not null,
	login_date timestamp default now()
)

select *
from users_2

insert into users_2(name, age, gender, salary)
values
	('Vlad', 23, true, 1011551051),
	('wfefwaer', 532, true, 1011551051),
	('Rrg', 12, true, 500000.164),
	('VFREvrr', 54, true, 47848.4),
	('FEVgver', 17, false, 999999.999);


truncate table users_2

create table users_3(
	id serial primary key,
	name varchar(25),
	age smallint not null check (age >= 0),
	gender boolean not null default true,
	salary numeric not null,
	login_date timestamp default now()
)

select *
from users_3

insert into users_3(name, age, gender, salary)
select name, age, gender, salary
from users_2
where salary = 999999.999

truncate table users_3
