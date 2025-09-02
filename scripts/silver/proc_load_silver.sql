-------------------------------------------------------------------------------------
-- Load silver.crm_cust_info
-- Cleanses and normalizes customer information from bronze layer
-------------------------------------------------------------------------------------

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info
    (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
    )
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname, -- Remove leading/trailing spaces
    TRIM(cst_lastname)  AS cst_lastname, -- Remove leading/trailing spaces

    -- Normalize marital status codes
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,

    -- Normalize gender codes
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,

    cst_create_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;  -- Keep only the most recent record per customer
GO


-------------------------------------------------------------------------------------
-- Load silver.crm_prd_info
-- Cleanses and derives product details from bronze layer
-------------------------------------------------------------------------------------

TRUNCATE TABLE silver.crm_prd_info;
GO

INSERT INTO silver.crm_prd_info
    (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
    -- dwh_create_date will auto-populate with GETDATE()
    )
SELECT
    prd_id,

    -- Category ID: first 5 characters of prd_key, replace "-" with "_"
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

    -- Product Key: strip prefix, keep the part after position 6
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,

    prd_nm,

    -- Default cost to 0 if NULL
    ISNULL(prd_cost, 0) AS prd_cost,

    -- Normalize product line codes
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,

    CAST(prd_start_dt AS DATE) AS prd_start_dt,

    -- Derive end date as (next product start date - 1 day)
    CAST(
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
        AS DATE
    ) AS prd_end_dt

FROM bronze.crm_prd_info;
GO


-------------------------------------------------------------------------------------
-- Load silver.crm_sales_details
-- Cleanses sales transactions and recalculates invalid values
-------------------------------------------------------------------------------------

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details
    (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
    )
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Validate order date: must be 8-digit YYYYMMDD format, else NULL
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,

    -- Validate ship date
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,

    -- Validate due date
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,

    -- Recalculate sales if NULL, <= 0, or doesn’t match (quantity * price)
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    -- Fix price if NULL or <= 0 → derive from sales ÷ quantity
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details;
GO


-------------------------------------------------------------------------------------
-- Load silver.erp_cust_az12
-- Standardizes ERP customer details from bronze layer
-------------------------------------------------------------------------------------

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12
    (
    cid,
    bdate,
    gen
    )
SELECT
    -- Standardize Customer ID by removing 'NAS' prefix if present
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
        ELSE cid
    END AS cid,

    -- Validate birthdate: replace future dates with NULL
    CASE
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    -- Normalize gender values
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen

FROM bronze.erp_cust_az12;
GO


-------------------------------------------------------------------------------------
-- Load silver.erp_loc_a101
-- Cleanses and standardizes customer location details
-------------------------------------------------------------------------------------

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101
    (
    cid,
    cntry
    )
SELECT
    -- Remove hyphens from Customer ID
    REPLACE(cid, '-', '') AS cid,

    -- Normalize and handle missing or blank country codes
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;
GO


-------------------------------------------------------------------------------------
-- Load silver.erp_px_cat_g1v2
-- Loads product category mappings without transformations
-------------------------------------------------------------------------------------

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2
    (
    id,
    cat,
    subcat,
    maintenance
    )
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;
GO
