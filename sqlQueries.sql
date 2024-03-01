
--Query 1:Quarterly Revenue Analysis and Trend Comparison Query

WITH QuarterlyRevenue AS (
    SELECT
        TO_CHAR(TO_TIMESTAMP(INVOICEDATE,'YYYY-MM-DD'), 'YYYY-Q') AS Quarter,
        SUM(QUANTITY * PRICE) AS TotalRevenue
    FROM TABLERETAIL
    GROUP BY TO_CHAR(TO_TIMESTAMP(INVOICEDATE,'YYYY-MM-DD'), 'YYYY-Q')
)
SELECT 
    Quarter,
   RANK() OVER (ORDER BY TotalRevenue desc) AS QuarterRank,

    LAG(TotalRevenue) OVER (ORDER BY Quarter) AS PreviousQuarterRevenue,
    TotalRevenue AS CurrentQuarterRevenue,
    LEAD(TotalRevenue) OVER (ORDER BY Quarter) AS NextQuarterRevenue
FROM 
    QuarterlyRevenue order by Quarter;
**************************************************************************************************
--Query 2:Top-Selling Stock Analysis Query: Ranking by Sales Volume

SELECT 
    STOCKCODE,
    SUM(QUANTITY) AS TOTAL_QUANTITY_SOLD,
    RANK() OVER (ORDER BY SUM(QUANTITY) DESC) AS top_sales
FROM     NSQL.TABLERETAIL
GROUP BY     STOCKCODE;
**************************************************************************************************
Query 3:Customer Popularity Analysis Query: Ranking Stocks by Unique Customer Purchases
SELECT 
    STOCKCODE,
    COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMER_COUNT,
    RANK() OVER (ORDER BY  COUNT(DISTINCT CUSTOMER_ID) DESC) AS POPULARITY_among_customers 
FROM 
    NSQL.TABLERETAIL
GROUP BY 
    STOCKCODE;
**************************************************************************************************
--Query 4:Monthly Sales Revenue Growth Analysis Query

SELECT 
    invoice_month,  monthly_revenue,
    LAG(monthly_revenue) OVER (ORDER BY TO_DATE(invoice_month, 'YYYY-MM')) AS prev_month_revenue,
    monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY TO_DATE(invoice_month, 'YYYY-MM')) AS revenue_growth
FROM (
    SELECT 
        TO_CHAR(TO_TIMESTAMP(INVOICEDATE,'YYYY-MM-DD'), 'YYYY-MM') AS invoice_month,
        SUM(PRICE * QUANTITY) AS monthly_revenue
    FROM 
        NSQL.TABLERETAIL
    GROUP BY 
        TO_CHAR(TO_TIMESTAMP(INVOICEDATE,'YYYY-MM-DD'), 'YYYY-MM')
) revenue_summary
ORDER BY   TO_DATE(invoice_month, 'YYYY-MM');
**************************************************************************************************
Query 5

WITH CustomerSpending AS (
    SELECT
        CUSTOMER_ID,
        SUM(QUANTITY * PRICE) AS TOTAL_SPENDING
    FROM
        tableRetail
    GROUP BY
        CUSTOMER_ID
)
SELECT
    CUSTOMER_ID,
    TOTAL_SPENDING,
    RANK() OVER (ORDER BY TOTAL_SPENDING DESC) AS CUSTOMER_RANK
FROM
    CustomerSpending
ORDER BY
    CUSTOMER_RANK;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------RFM Model------------------------------------------------------------------------------------------


-- implement a Monetary model for customers behavior for product purchasing
WITH cte AS (
    SELECT 
        Customer_ID, 
        SUM(Quantity * Price) AS TotalPaid, 
        COUNT(*) AS TotalPurchases,
        MAX((SELECT MAX(TO_TIMESTAMP(INVOICEDATE,'YYYY-MM-DD')) FROM tableretail)) - MAX(TO_TIMESTAMP(INVOICEDATE,'YYYY-MM-DD')) AS time_since_last_purchase
    FROM  
        tableretail
    GROUP BY 
        Customer_ID    
),
cte2 AS (
    SELECT 
        Customer_ID,
        NTILE(5) OVER (ORDER BY TotalPaid) AS montary,
        NTILE(5) OVER (ORDER BY TotalPurchases) AS freq,
        NTILE(5) OVER (ORDER BY time_since_last_purchase desc) AS r_score
    FROM 
        cte  
),
cte3 AS (
    SELECT  
        CUSTOMER_ID,
        r_score,
        ROUND((montary + freq) / 2) AS fm_score
    FROM 
        cte2
    ORDER BY  
        CUSTOMER_ID
),
cte4 AS (
    SELECT 
        CUSTOMER_ID,  r_score,
        fm_score,
        CASE
            WHEN (r_score = 5 AND fm_score = 5) OR (r_score = 4 AND fm_score = 5) OR (r_score = 5 AND fm_score = 4) THEN 'Champions'
            WHEN r_score = 5 AND fm_score = 3 THEN 'Loyal Customers'
            WHEN (r_score = 4 AND fm_score = 4) OR (r_score = 3 AND fm_score = 5) OR (r_score = 3 AND fm_score = 4) THEN 'Loyal Customers'
            WHEN (r_score = 5 AND fm_score = 2) OR (r_score = 4 AND fm_score = 3) OR (r_score = 3 AND fm_score = 3) OR (r_score = 4 AND fm_score = 2) THEN 'Potential Loyalists'
            WHEN r_score = 5 AND fm_score = 1 THEN 'Recent Customers'
            WHEN (r_score = 4 AND fm_score = 1) OR (r_score = 3 AND fm_score = 1) THEN 'Promising'
            WHEN r_score = 1 AND fm_score = 1 THEN 'Lost'
            WHEN r_score = 1 AND fm_score = 2 THEN 'Hibernating'
            WHEN (r_score = 3 AND fm_score IN (2, 3)) OR (r_score = 2 AND fm_score IN (3, 2)) OR (r_score = 2 AND fm_score = 2) THEN 'Customers Needing Attention'
            WHEN (r_score = 2 AND fm_score IN (5, 4)) OR (r_score = 1 AND fm_score = 3) THEN 'At Risk'
            WHEN (r_score = 1 AND fm_score IN (5, 4)) THEN 'Cant Lose Them'
            WHEN (r_score = 2 AND fm_score IN 1) THEN 'about to sleep'
            ELSE NULL 
        END AS CustomerSegment
    FROM    cte3
)
SELECT * FROM cte4

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------Daily Transaction------------------------------------------------------------------------------------------

--What is the maximum number of consecutive days a customer made purchases? 
 
WITH cte AS (
  SELECT  CALENDAR_DT, CUST_ID,
    NVL(CALENDAR_DT - LAG(CALENDAR_DT) OVER (PARTITION BY CUST_ID ORDER BY CALENDAR_DT), 1) AS data_gap
  FROM
    CUSTOMERS
),
cte2 AS (
  SELECT
    CALENDAR_DT,
    CUST_ID,data_gap,
    ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CALENDAR_DT) -
    ROW_NUMBER() OVER (PARTITION BY CUST_ID, data_gap ORDER BY CALENDAR_DT) AS group_number
  FROM cte
),
cte3 AS (
  SELECT CUST_ID,
    group_number, COUNT(*) AS c_days
  FROM cte2
  GROUP BY CUST_ID, group_number
)
SELECT
  CUST_ID, MAX(c_days) AS max_days
FROM cte3
GROUP BY CUST_ID order by CUST_ID ;


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------days/transactions does it take a customer to reach a spent threshold of 250 L.E------------------------------------------------------------------------------------------

--On average, How many days/transactions does it take a customer to reach a spent threshold of 250 L.E?

WITH CTE AS (
    SELECT
        cust_id,
        calendar_dt,
        SUM(amt_le) OVER (PARTITION BY cust_id ORDER BY calendar_dt) AS total,
        calendar_dt - FIRST_VALUE(calendar_dt) OVER (PARTITION BY cust_id ORDER BY calendar_dt) 
        AS thresholdDAYS
    FROM
        customers
),
CTE2 AS (
    SELECT 
        MIN(thresholdDAYS) AS thresholdDAYS, 
        cust_id 
    FROM
        (SELECT
            cust_id,
            calendar_dt,
            total,
            thresholdDAYS
        FROM
            CTE
        WHERE
            total >= 250) 
    GROUP BY 
        cust_id
)
SELECT   
  ROUND (AVG(thresholdDAYS)) AS avg_thresholdDAYS 
FROM 
    CTE2;






