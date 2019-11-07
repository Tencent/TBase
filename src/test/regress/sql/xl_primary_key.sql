--PRIMARY KEY/UNIQUE INDEX 
--For hash or modulo distributed table, first column of the primary must be same as distribution column --?? - needs to be updated

-- default is to distribute to all nodes 

-- this condition (distribution by second column of PRIMARY KEY ) is not erroneous. It is supported.
 
CREATE TABLE xl_pk_products ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid, product_no) ) DISTRIBUTE BY HASH(product_no);

CREATE TABLE xl_pk_products1 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid, product_no) ) DISTRIBUTE BY HASH(product_uid);

CREATE TABLE xl_pk_products2 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid) ) DISTRIBUTE BY HASH(product_no); 

CREATE TABLE xl_pk_products3 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_no, product_uid) ) DISTRIBUTE BY MODULO(product_no);

CREATE TABLE xl_pk_products4 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid, product_no) ) DISTRIBUTE BY MODULO(product_no);

CREATE TABLE xl_pk_products5 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid) ) DISTRIBUTE BY MODULO(product_no);

--For roundrobin distributed table, PRIMARY KEY or UNIQUE INDEX is not supported

CREATE TABLE xl_pk_products6 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid) ) DISTRIBUTE BY ROUNDROBIN;

CREATE TABLE xl_pk_products7 ( product_no integer, product_uid integer, name text, price numeric ) DISTRIBUTE BY ROUNDROBIN;

CREATE UNIQUE INDEX product_uuid ON xl_pk_products7 (product_uid, product_no); 

DROP TABLE xl_pk_products; 
DROP TABLE xl_pk_products1; 
DROP TABLE xl_pk_products2; 
DROP TABLE xl_pk_products3; 
DROP TABLE xl_pk_products4; 
DROP TABLE xl_pk_products5; 
DROP TABLE xl_pk_products6; 
DROP TABLE xl_pk_products7;
