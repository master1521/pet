-- Проверка каталогов
SHOW CATALOGS;

-- Показать namespaces
SHOW SCHEMAS IN iceberg;

-- Показать таблицы
SHOW TABLES IN iceberg.sandbox;


-- Создать namespace
CREATE SCHEMA IF NOT EXISTS iceberg.dev;


-- Удалить если существует
DROP TABLE IF EXISTS iceberg.sandbox.test_trino;


-- Создать таблицу отзывов с партиционированием
CREATE TABLE iceberg.sandbox.customer_reviews (
    review_id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    rating INTEGER,
    review_text VARCHAR,
    helpful_count INTEGER,
    review_date TIMESTAMP(6) WITH TIME ZONE,
    is_verified BOOLEAN
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(review_date)']
);



-- Сгенерировать 50,000 отзывов из существующих данных
INSERT INTO iceberg.sandbox.customer_reviews
SELECT 
    ROW_NUMBER() OVER (ORDER BY o.order_id) as review_id,
    o.user_id,
    o.product_id,
    CASE 
        WHEN RANDOM() < 0.4 THEN 5
        WHEN RANDOM() < 0.6 THEN 4
        WHEN RANDOM() < 0.85 THEN 3
        WHEN RANDOM() < 0.95 THEN 2
        ELSE 1
    END as rating,
    CASE 
        WHEN RANDOM() < 0.4 THEN 'Excellent product! Highly recommend'
        WHEN RANDOM() < 0.6 THEN 'Good quality, satisfied with purchase'
        WHEN RANDOM() < 0.85 THEN 'Average product, nothing special'
        WHEN RANDOM() < 0.95 THEN 'Disappointed with quality'
        ELSE 'Terrible product, waste of money'
    END as review_text,
    CAST(RANDOM() * 100 AS INTEGER) as helpful_count,
    o.order_date + INTERVAL '1' DAY * CAST(RANDOM() * 30 AS INTEGER) as review_date,
    RANDOM() < 0.9 as is_verified
FROM iceberg.sandbox.orders o
WHERE o.status = 'completed'
LIMIT 50000;

SELECT *
FROM iceberg.sandbox.customer_reviews
LIMIT 1000

-- Посчитать строки
SELECT COUNT(*) as total_reviews 
FROM iceberg.sandbox.customer_reviews;

-- Распределение по рейтингам
SELECT 
    rating,
    COUNT(*) as count,
    ROUND(AVG(helpful_count), 2) as avg_helpful,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percent
FROM iceberg.sandbox.customer_reviews
GROUP BY rating
ORDER BY rating DESC;

-- Первые 10 записей
SELECT * 
FROM iceberg.sandbox.customer_reviews 
LIMIT 10;



-- 				CRUD ОПЕРАЦИИ
-- Агрегация по месяцам
SELECT 
    DATE_TRUNC('month', review_date) as month,
    COUNT(*) as reviews_count,
    AVG(rating) as avg_rating
FROM iceberg.sandbox.customer_reviews
GROUP BY DATE_TRUNC('month', review_date)
ORDER BY month;


-- Вставить одну строку
INSERT INTO iceberg.sandbox.customer_reviews VALUES (
    100001,
    5678,
    234,
    5,
    'Amazing product from DBeaver test!',
    25,
    TIMESTAMP '2024-12-05 12:00:00',
    true
);

-- Вставить несколько строк
INSERT INTO iceberg.sandbox.customer_reviews VALUES
    (100002, 1111, 222, 4, 'Good', 10, TIMESTAMP '2024-12-05 13:00:00', true),
    (100003, 2222, 333, 3, 'OK', 5, TIMESTAMP '2024-12-05 14:00:00', false);



-- Увеличить helpful_count для высоких рейтингов
UPDATE iceberg.sandbox.customer_reviews
SET helpful_count = helpful_count + 10
WHERE rating = 5 
  AND review_date >= DATE '2024-11-01';

-- Изменить статус проверки
UPDATE iceberg.sandbox.customer_reviews
SET is_verified = true
WHERE review_id IN (100001, 100002, 100003);


-- Удалить конкретный отзыв
DELETE FROM iceberg.sandbox.customer_reviews
WHERE review_id = 100001;

-- Удалить старые отзывы
DELETE FROM iceberg.sandbox.customer_reviews
WHERE review_date < DATE '2024-06-01';

-- Удалить негативные непроверенные
DELETE FROM iceberg.sandbox.customer_reviews
WHERE rating <= 2 
  AND is_verified = false;



-- Посмотреть историю snapshots
SELECT * FROM "iceberg.sandbox.customer_reviews$snapshots"
ORDER BY committed_at DESC;


-- 				АНАЛИТИЧЕСКИЕ ЗАПРОСЫ ДЛЯ DATA LAKEHOUSE

-- Проверка распределения
SELECT 
    rating, 
    COUNT(*) AS cnt, 
    AVG(helpful_count) AS avg_helpful
FROM iceberg.sandbox.customer_reviews
GROUP BY rating
ORDER BY rating


-- Получить статистику по странам
SELECT 
    country,
    COUNT(*) as users_count,
    AVG(age) as avg_age
FROM iceberg.sandbox.users
GROUP BY country
ORDER BY users_count DESC
  
-- Чтение с фильтром по месяцу (partition pruning)
SELECT
    COUNT(*) AS cnt,
    MIN(review_date) AS min_dt,
    MAX(review_date) AS max_dt
FROM iceberg.sandbox.customer_reviews
WHERE review_date >= DATE '2024-05-01' AND review_date < DATE '2024-06-01'



-- Средний рейтинг по категориям товаров
SELECT 
    p.category,
    COUNT(r.review_id) as reviews_count,
    AVG(r.rating) as avg_rating,
    SUM(r.helpful_count) as total_helpful
FROM iceberg.sandbox.customer_reviews r
JOIN iceberg.sandbox.products p ON r.product_id = p.product_id
GROUP BY p.category
ORDER BY avg_rating DESC


-- Топ товары по отзывам
SELECT 
    p.product_name,
    p.category,
    COUNT(r.review_id) as reviews_count,
    AVG(r.rating) as avg_rating
FROM iceberg.sandbox.customer_reviews r
JOIN iceberg.sandbox.products p ON r.product_id = p.product_id
GROUP BY p.product_name, p.category
HAVING COUNT(r.review_id) >= 5
ORDER BY avg_rating DESC, reviews_count DESC
LIMIT 10


-- Активные рецензенты
SELECT
    u.name,
    u.country,
    COUNT(r.review_id) as reviews_written,
    AVG(r.rating) as avg_rating_given
FROM iceberg.sandbox.customer_reviews r
JOIN iceberg.sandbox.users u ON r.user_id = u.user_id
GROUP BY u.name, u.country
ORDER BY reviews_written DESC
LIMIT 10

