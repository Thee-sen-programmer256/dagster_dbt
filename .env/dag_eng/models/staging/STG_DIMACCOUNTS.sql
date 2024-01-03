
WITH CTE AS (
    SELECT * FROM {{ref('ACCOUNTS')}}
),
CTE2 AS (
    SELECT *  FROM {{source('dagster','CUSTOMERS')}}
)
,FINAL AS (
    SELECT A.customer_id AS CUSTOMER_ID,
           first_name AS  FIRST_NAME,
           last_name AS LAST_NAME,
           email AS EMAIL,
           phone AS PHONE,
           account_number AS ACCOUNT_NUMBER,
           balance AS ACCOUNT_BALANCE,
           account_type AS ACCOUNT_TYPE FROM CTE A
    LEFT OUTER JOIN CTE2 B ON B.customer_id=A.customer_id
)
SELECT * FROM FINAL