--creation of bronze layer + stored procedure
use warehouse;
if OBJECT_ID('BRONZE.crm_cust_info','u') is not null
  DROP TABLE BRONZE.crm_cust_info;
create table BRONZE.crm_cust_info(
 cst_id int,
 cst_key NvARCHAR(50),
 cst_firstname varchar(50),
 cst_lastname nvarchar(50),
 cst_marital_status nvarchar(50),
 cst_gender nvarchar(50),
 cst_create_date date,
);
if OBJECT_ID('BRONZE.crm_prd_info','u') is not null
drop table BRONZE.crm_prd_info;
create table BRONZE.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
);
if OBJECT_ID('BRONZE.crm_sales_details','u') is not null
  drop table BRONZE.crm_sales_details;
create table BRONZE.crm_sales_details(
 sls_ord_num nvarchar(50),
 sls_prd_key nvarchar(50),
 sls_cust_id int,
 sls_order_id int,
 sls_ship_dt int,
 sls_due_dt int,
 sls_sales int,
 sls_quantity int,
 sls_price  int,
 );


if OBJECT_ID('BRONZE.erp_cust_AZ12','u') is not null
  drop table  BRONZE.erp_cust_AZ12;
create table BRONZE.erp_cust_AZ12(
cid nvarchar(50),
bdate date,
gen nvarchar(50),
);
if OBJECT_ID('BRONZE.erp_loc_A101','u') is not null
drop table BRONZE.erp_loc_A101;
create table BRONZE.erp_loc_A101(
cid nvarchar(50),
cntry nvarchar(50),
);
if OBJECT_ID('BRONZE.erp_px_cat_g1v2','u') is not null
drop table  BRONZE.erp_px_cat_g1v2;
create table BRONZE.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50),
);
/* CREATED THE TABLES NOW BULK INSERTING DATA USING A PROCEDURE*/
exec bronze.load_bronze
go
create or alter procedure bronze.load_bronze as
begin
     declare @start_time datetime,@end_time datetime
     begin try
		print' ------------------------------';
		print '----bronze layer------------';
		print '----loding data-----------------';

		set @start_time=GETDATE()
		truncate table bronze.crm_cust_info;
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
		 firstrow= 2,
		 fieldterminator=',',
		 tablock
		 );
		 set @end_time=GETDATE()
		 print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

		 set @start_time=GETDATE()
		 truncate table bronze.crm_prd_info;
		 bulk insert bronze.crm_prd_info
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		 firstrow= 2,
		 fieldterminator=',',
		 tablock
		 );
		 set @end_time=GETDATE()
		  print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

		 set @start_time=GETDATE()
		truncate table bronze.crm_sales_details;
		bulk insert bronze.crm_sales_details
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
		 firstrow= 2,
		 fieldterminator=',',
		 tablock
		 );
		 set @end_time=GETDATE()
		  print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

		 set @start_time=GETDATE()
		 truncate table bronze.erp_cust_AZ12;
		 bulk insert bronze.erp_cust_AZ12
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
		with(
		 firstrow= 2,
		 fieldterminator=',',
		 tablock
		 );
		 set @end_time=GETDATE()
		  print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

		 set @start_time=GETDATE()
		  truncate table bronze.erp_loc_A101;
		 bulk insert bronze.erp_LOC_A101
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
		with(
		 firstrow= 2,
		 fieldterminator=',',
		 tablock
		 );
		 set @end_time=GETDATE()
		  print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

		 set @start_time=GETDATE()
		  truncate table bronze.erp_px_cat_g1v2;
		 bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Admin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV'
		with(
		 firstrow= 2,
		 fieldterminator=',',
		 tablock
		 );
		 set @end_time=GETDATE()
		 print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

	end try
    begin catch
	print '------error occurec-----';
	print 'error is'+error_message();
	end catch
end

