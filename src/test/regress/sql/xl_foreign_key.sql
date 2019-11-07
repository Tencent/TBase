--FOREIGN KEY 

--For a distributed table, the REFERENCES clause must use the distribution column (product_no here).
-- As for ROUNDROBIN, primary key and unique index are not supported, this test does not include it.
-- default distributed by HASH on product_no
CREATE TABLE xl_fk_products (
    product_no integer PRIMARY KEY,
    product_id integer,
    name text,
    price numeric
);

CREATE TABLE xl_fk_products01 (
    product_no integer,
    product_id integer PRIMARY KEY,
    name text,
    price numeric
);

CREATE TABLE xl_fk_products1 (
    product_no integer PRIMARY KEY,
    product_id integer ,
    name text,
    price numeric
) DISTRIBUTE BY MODULO(product_no);

CREATE TABLE xl_fk_products11 (
    product_no integer ,
    product_id integer PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY MODULO(product_id);

CREATE TABLE xl_fk_orders (
    order_id integer,
    product_no integer REFERENCES xl_fk_products (product_no),
    quantity integer
) DISTRIBUTE BY HASH(product_no);
--pass

CREATE TABLE xl_fk_orders01 (
    order_id integer,
    product_no integer REFERENCES xl_fk_products (product_no),
    quantity integer
) DISTRIBUTE BY MODULO(product_no);
--fail - as xl_fk_products has product_no distributed by HASH

CREATE TABLE xl_fk_orders02 (
    order_id integer,
    product_no integer REFERENCES xl_fk_products1 (product_no),
    quantity integer
) DISTRIBUTE BY MODULO(product_no);
--pass - as source is also modulo distributed

CREATE TABLE xl_fk_orders1 (
    order_id integer,
    product_no integer REFERENCES xl_fk_products (product_no),
    quantity integer
) DISTRIBUTE BY HASH(order_id); -- fail

CREATE TABLE xl_fk_orders2 (
    order_id integer,
    product_no integer REFERENCES xl_fk_products (product_no),
    quantity integer
) DISTRIBUTE BY MODULO(order_id); -- fail

--This implies that PRIMARY KEY and REFERENCES cannot use different columns

CREATE TABLE xl_fk_orders3 (
    order_id integer,
    product_no integer REFERENCES xl_fk_products (product_id),
    quantity integer
) DISTRIBUTE BY HASH(order_id); 
-- fail - as references is using a different column that primary key of referred table

CREATE TABLE xl_fk_orders4 (
    order_id integer,
    product_no integer REFERENCES xl_fk_products1 (product_id),
    quantity integer
) DISTRIBUTE BY MODULO(order_id); 
-- fail- as references is using a different column that primary key of referred table

--This also implies that more than one FOREIGN KEY constraints cannot be specified


CREATE TABLE xl_fk_orders5 (
    order_id integer,
    product_no integer REFERENCES xl_fk_products (product_no),
    product_id integer REFERENCES xl_fk_products01 (product_id),
    quantity integer
) DISTRIBUTE BY HASH(product_no); 

CREATE TABLE xl_fk_orders6 (
    order_id integer,
    product_no integer REFERENCES xl_fk_products1 (product_no),
    product_id integer REFERENCES xl_fk_products11 (product_id),
    quantity integer
) DISTRIBUTE BY MODULO(order_id); 


DROP TABLE xl_fk_orders;
DROP TABLE xl_fk_orders01;
DROP TABLE xl_fk_orders02;
DROP TABLE xl_fk_orders1;
DROP TABLE xl_fk_orders2;
DROP TABLE xl_fk_orders3;
DROP TABLE xl_fk_orders4;
DROP TABLE xl_fk_orders5;
DROP TABLE xl_fk_orders6;
DROP TABLE xl_fk_products;
DROP TABLE xl_fk_products01;
DROP TABLE xl_fk_products1;
DROP TABLE xl_fk_products11;


