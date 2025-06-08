--Основная таблица
CREATE TABLE products_2 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    price NUMERIC,
    updated_at TIMESTAMP DEFAULT now()
);

select *
from products_2

--Таблица отслеживания изменений (журнал изменений)
CREATE TABLE products_cdc_log (
    id SERIAL PRIMARY KEY,
    product_id INTEGER,
    operation VARCHAR(10),
    old_price NUMERIC,
    new_price NUMERIC,
    changed_at TIMESTAMP DEFAULT now()
);

select *
from products_cdc_log


CREATE TRIGGER trg_products_cdc
AFTER INSERT OR UPDATE OR DELETE ON products_2
FOR EACH ROW
EXECUTE FUNCTION log_products_cdc();

--Функция для триггера
CREATE OR REPLACE FUNCTION log_products_cdc()
RETURNS TRIGGER AS $$
BEGIN
    --TG_OP — зарезервированное системное имя переменной.
    IF TG_OP = 'INSERT' THEN
        INSERT INTO products_cdc_log (product_id, operation, new_price)
        VALUES (NEW.id, 'INSERT', NEW.price);

    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO products_cdc_log (product_id, operation, old_price, new_price)
        VALUES (OLD.id, 'UPDATE', OLD.price, NEW.price);

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO products_cdc_log (product_id, operation, old_price)
        VALUES (OLD.id, 'DELETE', OLD.price);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


--Изменение основной таблицы влечёт за собой вызов тригера
--Тригер вызывает функцию, которая записывает изменения в таблицу products_cdc_log
insert into products_2(name, price)
values
	('Notebook', 2433541),
	('Notebook_2', 2433541),
	('Notebook_3', 2433541),
	('Notebook_4', 2433541)

update products_2
set price = 99999
where name = 'Notebook_3'

delete from products_2
where name = 'Notebook_4'

--Проверка
SELECT * FROM products_cdc_log
WHERE changed_at >= NOW() - INTERVAL '1 day'
ORDER BY changed_at;