/* define tables in silver layer*/
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

if OBJECT_ID('silver.crm_cust_info','u') is not null
  DROP TABLE silver.crm_cust_info;
create table silver.crm_cust_info(
 cst_id int,
 cst_key NvARCHAR(50),
 cst_firstname varchar(50),
 cst_lastname nvarchar(50),
 cst_marital_status nvarchar(50),
 cst_gender nvarchar(50),
 cst_create_date date,
 dwh_create_date datetime2 default getdate()
);


if OBJECT_ID('silver.crm_prd_info','u') is not null
drop table silver.crm_prd_info;
create table silver.crm_prd_info(
prd_id int,
cat_id nvarchar(50),
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()
);


if OBJECT_ID('silver.crm_sales_details','u') is not null
  drop table silver.crm_sales_details;
create table silver.crm_sales_details(
 sls_ord_num nvarchar(50),
 sls_prd_key nvarchar(50),
 sls_cust_id int,
 sls_order_id date,
 sls_ship_dt date,
 sls_due_dt date,
 sls_sales int,
 sls_quantity int,
 sls_price  int,
 dwh_create_date datetime2 default getdate()
 );

if OBJECT_ID('silver.erp_cust_AZ12','u') is not null
  drop table  silver.erp_cust_AZ12;
create table silver.erp_cust_AZ12(
cid nvarchar(50),
bdate date,
gen nvarchar(50),
dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.erp_loc_A101','u') is not null
drop table silver.erp_loc_A101;
create table silver.erp_loc_A101(
cid nvarchar(50),
cntry nvarchar(50),
dwh_create_date datetime2 default getdate()
);


if OBJECT_ID('silver.erp_px_cat_g1v2','u') is not null
drop table  silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50),
dwh_create_date datetime2 default getdate()
);