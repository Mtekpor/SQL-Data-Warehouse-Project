
/*
==================================================================================================
STORE PROCEDURE: Load Bronze Layer ( Source->Bronze
==================================================================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performa the following actions:
	-Truncates the bronze tables before loading data.
	- Use the 'BULK INSERT' command to load data from CSV files to bronze tables.
Parameters:
	None.
	This stored procedure does not accept any parameters or returns any values.

Usage Example: 
	EXEC bronze.load_bronze;
===================================================================================================
*/
--- Creating store procedures----
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
	
	BEGIN
		-- Tracking ETL duration ( to detact and track bottlenecks )
		DECLARE @start_time DATETIME, @end_time DATETIME;

		BEGIN TRY 

			PRINT '=============================================='
			PRINT 'Loading the bronze layer'
			PRINT '=============================================='
	

			PRINT '=============================================='
			PRINT '>> Truncating CRM Tables & Inserting data'
			PRINT '=============================================='

			--Loading bronze layer dataset from source locations--

			SET @start_time = GETDATE();
			-- load 1 --
			TRUNCATE TABLE bronze.crm_cust_info;
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\sql_pc\dwh_project\datasets\source_crm\cust_info.csv'
			WITH
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------'
			
			--load 2--
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.crm_prd_info;
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\sql_pc\dwh_project\datasets\source_crm\prd_info.csv'
			WITH
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------'


			--load 3--
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.crm_sales_details;
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\sql_pc\dwh_project\datasets\source_crm\sales_details.csv'
			WITH
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
				SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------'
	

			PRINT '=============================================='
			PRINT '>>Truncating ERP Tables & Inserting data'
			PRINT '=============================================='

			--load 4--
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.erp_cust_az12;
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\sql_pc\dwh_project\datasets\source_erp\cust_az12.csv'
			WITH
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
				SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------'


			--load 5--
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.erp_loc_a101;
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\sql_pc\dwh_project\datasets\source_erp\loc_a101.csv'
			WITH
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
				SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------'


			--load 6--
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\sql_pc\dwh_project\datasets\source_erp\px_cat_g1v2.csv'
			WITH
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
				SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------'
		
		END TRY

		BEGIN CATCH
		PRINT '=============================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=============================================='
		END CATCH
	END
