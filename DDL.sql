#part 1
DROP TABLE if exists tsla;
CREATE TABLE tsla
(
  order_id           VARCHAR(1000),
  model              VARCHAR(1000),
  reservation_date   VARCHAR(1000),
  first_name         VARCHAR(1000),
  last_name          VARCHAR(1000),
  street_addr        VARCHAR(3000),
  city               VARCHAR(1000),
  zipcode            INT,
  country_code       VARCHAR(50)
);

#part 2
DROP TABLE if exists tsla_orders;
CREATE TABLE tsla_orders
(
  order_id           VARCHAR(1000),
  model              VARCHAR(1000),
  reservation_date   VARCHAR(1000),
  order_status       VARCHAR(1000),
  tsp                TIMESTAMP(3)
);

#part 2
DROP TABLE if exists tsla_order_status;
CREATE TABLE tsla_order_status
(
  max_tsp              TIMESTAMP(3),
  ct_order_finalized   INT,
  ct_order_confirmed   INT,
  ct_order_cancelled   INT,
  ct_order_placed      INT
);

#part 2 view
DROP VIEW if exists tsla_orders_vw;
CREATE VIEW tsla_orders_vw
AS
(
SELECT o.order_id,
       rr.order_status,
       o.model,
       o.reservation_date
FROM tsla_orders o
  JOIN (SELECT *
        FROM (SELECT order_id,
                     order_status,
                     ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY tsp DESC) AS rn
              FROM tsla_orders) res
        WHERE res.rn = 1) rr ON o.order_id = rr.order_id
WHERE o.model IS NOT NULL
);


