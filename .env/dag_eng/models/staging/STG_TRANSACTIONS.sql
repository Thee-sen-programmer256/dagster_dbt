WITH CTE AS (
  SELECT * FROM {{ ref('ACCOUNTS') }}
),
CTE2 AS (
  SELECT * FROM {{ ref('TRANSACTIONS') }}
),
FINAL AS (
  SELECT
    A.customer_id AS CUSTOMER_ID,
    A.account_number AS ACCOUNT_NUMBER,
    B.transaction_id AS TRANSACTION_ID,
    B.amount AS TRANSACTION_AMOUNT,
    B.transaction_type AS TRANSACTION_TYPE
  FROM CTE A 
  LEFT OUTER JOIN CTE2 B ON A.customer_id = B.customer_id
)
SELECT * FROM FINAL