WITH CTE AS (
SELECT * FROM ({{source('dagster_dbt','ACCOUNTS')}})
),
CTE2 AS (
SELECT * FROM ({{source('dagster_dbt','TRANSACTIONS')}})
),
FINAL AS (
SELECT
A.customer_id AS CUSTOMER_ID,
account_number AS ACCOUNT_NUMBER,
transaction_id AS TRANSACTION_ID,
amount AS TRANSACTION_AMOUNT,
transaction_type AS TRANSACTION_TYPE
FROM CTE A 
LEFT OUTER JOIN CTE2 B ON B.customer_id = A.customer_id
)
SELECT * FROM FINAL

