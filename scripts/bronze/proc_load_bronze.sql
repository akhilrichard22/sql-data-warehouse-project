-----------------------------------------
-- 0️⃣ Ensure Master Key exists
-----------------------------------------
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPasswordHere123!';
    PRINT 'Master Key Created';
END
ELSE
BEGIN
    PRINT 'Master Key already exists';
END
GO

-----------------------------------------
-- 1️⃣ Drop External Data Source if exists
-----------------------------------------
IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'BlobStorage')
BEGIN
    DROP EXTERNAL DATA SOURCE BlobStorage;
    PRINT 'Existing External Data Source dropped';
END
ELSE
BEGIN
    PRINT 'External Data Source does not exist';
END
GO

-----------------------------------------
-- 2️⃣ Drop Database Scoped Credential if exists
-----------------------------------------
IF EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'AzureBlobSas')
BEGIN
    DROP DATABASE SCOPED CREDENTIAL AzureBlobSas;
    PRINT 'Existing Credential dropped';
END
ELSE
BEGIN
    PRINT 'Credential does not exist';
END
GO

-----------------------------------------
-- 3️⃣ Create Database Scoped Credential
-----------------------------------------
CREATE DATABASE SCOPED CREDENTIAL AzureBlobSas
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
-- Always check SAS token expiry
SECRET = 'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-08-29T00:29:54Z&st=2025-08-28T16:14:54Z&spr=https&sig=%2FnrJTCGtgivrUIYz7wNU3OEXsV6pENLhke6vlmp6%2Bes%3D';
PRINT 'Database Scoped Credential created';
GO

-----------------------------------------
-- 4️⃣ Create External Data Source
-----------------------------------------
CREATE EXTERNAL DATA SOURCE BlobStorage
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://warehouseprojectdatafile.blob.core.windows.net/datasets',
    CREDENTIAL = AzureBlobSas
);
PRINT 'External Data Source created';
GO

-----------------------------------------
-- 5️⃣ Load bronze.crm_cust_info
-----------------------------------------
BEGIN TRY
    PRINT 'Loading bronze.crm_cust_info...';

    TRUNCATE TABLE bronze.crm_cust_info;

    BULK INSERT bronze.crm_cust_info
    FROM 'cust_info.csv'
    WITH (
        DATA_SOURCE = 'BlobStorage',
        FORMAT = 'CSV',
        FIRSTROW = 2,
        TABLOCK
    );

    PRINT 'crm_cust_info loaded successfully';
END TRY
BEGIN CATCH
    PRINT 'Error loading crm_cust_info: ' + ERROR_MESSAGE();
END CATCH
GO

-----------------------------------------
-- 5️⃣ Load bronze.crm_prd_info
-----------------------------------------
BEGIN TRY
    PRINT 'Loading bronze.crm_prd_info...';

    TRUNCATE TABLE bronze.crm_prd_info;

    BULK INSERT bronze.crm_prd_info
    FROM 'prd_info.csv'
    WITH (
        DATA_SOURCE = 'BlobStorage',
        FORMAT = 'CSV',
        FIRSTROW = 2,
        TABLOCK
    );

    PRINT 'crm_prd_info loaded successfully';
END TRY
BEGIN CATCH
    PRINT 'Error loading crm_prd_info: ' + ERROR_MESSAGE();
END CATCH
GO

-----------------------------------------
-- 5️⃣ Load bronze.crm_sales_details
-----------------------------------------
BEGIN TRY
    PRINT 'Loading bronze.crm_sales_details...';

    TRUNCATE TABLE bronze.crm_sales_details;

    BULK INSERT bronze.crm_sales_details
    FROM 'sales_details.csv'
    WITH (
        DATA_SOURCE = 'BlobStorage',
        FORMAT = 'CSV',
        FIRSTROW = 2,
        TABLOCK
    );

    PRINT 'crm_sales_details loaded successfully';
END TRY
BEGIN CATCH
    PRINT 'Error loading crm_sales_details: ' + ERROR_MESSAGE();
END CATCH
GO

-----------------------------------------
-- 5️⃣ Load bronze.erp_loc_a101
-----------------------------------------
BEGIN TRY
    PRINT 'Loading bronze.erp_loc_a101...';

    TRUNCATE TABLE bronze.erp_loc_a101;

    BULK INSERT bronze.erp_loc_a101
    FROM 'LOC_A101.csv'
    WITH (
        DATA_SOURCE = 'BlobStorage',
        FORMAT = 'CSV',
        FIRSTROW = 2,
        TABLOCK
    );

    PRINT 'erp_loc_a101 loaded successfully';
END TRY
BEGIN CATCH
    PRINT 'Error loading erp_loc_a101: ' + ERROR_MESSAGE();
END CATCH
GO

-----------------------------------------
-- 5️⃣ Load bronze.erp_cust_az12
-----------------------------------------
BEGIN TRY
    PRINT 'Loading bronze.erp_cust_az12...';

    TRUNCATE TABLE bronze.erp_cust_az12;

    BULK INSERT bronze.erp_cust_az12
    FROM 'CUST_AZ12.csv'
    WITH (
        DATA_SOURCE = 'BlobStorage',
        FORMAT = 'CSV',
        FIRSTROW = 2,
        TABLOCK
    );

    PRINT 'erp_cust_az12 loaded successfully';
END TRY
BEGIN CATCH
    PRINT 'Error loading erp_cust_az12: ' + ERROR_MESSAGE();
END CATCH
GO

-----------------------------------------
-- 5️⃣ Load bronze.erp_px_cat_g1v2
-----------------------------------------
BEGIN TRY
    PRINT 'Loading bronze.erp_px_cat_g1v2...';

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'PX_CAT_G1V2.csv'
    WITH (
        DATA_SOURCE = 'BlobStorage',
        FORMAT = 'CSV',
        FIRSTROW = 2,
        TABLOCK
    );

    PRINT 'erp_px_cat_g1v2 loaded successfully';
END TRY
BEGIN CATCH
    PRINT 'Error loading erp_px_cat_g1v2: ' + ERROR_MESSAGE();
END CATCH
GO
