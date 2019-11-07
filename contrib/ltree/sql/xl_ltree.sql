
--Column of types “citext”, “ltree” cannot be used as distribution column
--ltree - labels of data stored in a hierarchical tree-like structure
CREATE TABLE xl_dc27 (
    product_no integer,
    product_id ltree PRIMARY KEY,
    name MONEY,
    purchase_date TIMETZ,
    price numeric
) DISTRIBUTE BY HASH (product_id); --fail
