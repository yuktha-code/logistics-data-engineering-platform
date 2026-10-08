DROP VIEW IF EXISTS mart_city_performance;
CREATE VIEW mart_city_performance AS
SELECT c.city, c.state,
 COUNT(*) AS total_orders,
 SUM(CASE WHEN f.status='DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
 SUM(CASE WHEN f.status='CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
 ROUND(100.0*SUM(CASE WHEN f.status='DELIVERED' AND f.on_time=1 THEN 1 ELSE 0 END) /
   NULLIF(SUM(CASE WHEN f.status='DELIVERED' THEN 1 ELSE 0 END),0),2) AS on_time_pct,
 ROUND(AVG(CASE WHEN f.status='DELIVERED' THEN f.delay_minutes END),2) AS avg_delay_minutes,
 ROUND(AVG(f.distance_km),2) AS avg_distance_km,
 ROUND(SUM(f.fare_inr),2) AS synthetic_gross_fare_inr
FROM fact_delivery f JOIN dim_city c ON f.city_id=c.city_id GROUP BY c.city,c.state;

DROP VIEW IF EXISTS mart_delivery_daily;
CREATE VIEW mart_delivery_daily AS
SELECT SUBSTR(created_at,1,10) AS order_day, city_id, COUNT(*) AS orders,
 SUM(CASE WHEN status='DELIVERED' THEN 1 ELSE 0 END) AS delivered,
 SUM(CASE WHEN status='DELIVERED' AND on_time=1 THEN 1 ELSE 0 END) AS delivered_on_time,
 ROUND(100.0*SUM(CASE WHEN status='DELIVERED' AND on_time=1 THEN 1 ELSE 0 END) /
 NULLIF(SUM(CASE WHEN status='DELIVERED' THEN 1 ELSE 0 END),0),2) AS on_time_pct,
 SUM(CASE WHEN status='CANCELLED' THEN 1 ELSE 0 END) AS cancelled,
 SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed
FROM fact_delivery GROUP BY SUBSTR(created_at,1,10),city_id;

DROP VIEW IF EXISTS mart_delivery_exceptions;
CREATE VIEW mart_delivery_exceptions AS
SELECT f.order_id,c.city,f.status,f.promised_at,f.delivered_at,f.delay_minutes,
 CASE WHEN f.status='CANCELLED' THEN 'CANCELLED'
 WHEN f.status='FAILED' THEN 'FAILED'
 WHEN f.status='DELIVERED' AND f.on_time=0 THEN 'LATE'
 ELSE 'NONE' END AS exception_type
FROM fact_delivery f JOIN dim_city c ON f.city_id=c.city_id
WHERE f.status <> 'DELIVERED' OR f.on_time=0;

DROP VIEW IF EXISTS mart_driver_daily;
CREATE VIEW mart_driver_daily AS
SELECT SUBSTR(created_at,1,10) AS order_day,driver_id,
 COUNT(*) AS assigned_orders,
 SUM(CASE WHEN status='DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
 SUM(CASE WHEN status='DELIVERED' AND on_time=1 THEN 1 ELSE 0 END) AS delivered_on_time
FROM fact_delivery WHERE driver_id IS NOT NULL AND driver_id <> ''
GROUP BY SUBSTR(created_at,1,10),driver_id;
