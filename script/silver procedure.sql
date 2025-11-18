use warehouse
GO

-- Batch 1: CREATE OR ALTER PROCEDURE (Must be the first statement in this batch)
create or alter procedure silver.load_silver as 
Begin 
    declare @start_time datetime,@end_time datetime
    begin try
		print' ------------------------------';
		print '----silver layer------------';
		print '----loding data-----------------';
        
        -- crm_prd_info Load
        set @start_time=GETDATE()
        truncate  table silver.crm_prd_info
        insert into silver.crm_prd_info(
        prd_id , cat_id , prd_key , prd_nm , prd_cost , prd_line , prd_start_dt , prd_end_dt 
        )
        select 
        prd_id,
        replace(substring(prd_key,1,5),'-','_') as cat_id,
        substring(prd_key,7,len(prd_key)) as prd_key,
        prd_nm,
        isnull(prd_cost,0) as prd_cost,
        case  upper(trim(prd_line))
             when 'M' then 'Mountain'
             when 'R' then 'Road'
             when 'S' then 'Other sales'
             when 'T' then 'Touring'
             else 'n/a'
        end as prd_line,
        cast (prd_start_dt as date) as prd_start_dt,
        CAST(
            DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) 
        AS DATE) AS prd_end_dt -- Corrected the alias here from prd_end_dt_test to prd_end_dt
        from bronze.crm_prd_info
        set @end_time=GETDATE()
		 print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

        -- crm_cust_info Load
        set @start_time=GETDATE()
        truncate  table silver.crm_cust_info
        insert into silver.crm_cust_info(
         cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gender, cst_create_date)
        select cst_id,cst_key,
        trim(cst_firstname) as cst_firstname,
        trim(cst_lastname) as cst_lastname,
        case when upper(TRIM(cst_marital_status))='S' then 'Single'
             when upper(TRIM(cst_marital_status))='M' then 'Married'
             else 'n/a'
        end cst_marital_status,
        case when upper(TRIM(cst_gender))='F' then 'Female'
             when upper(TRIM(cst_gender))='M' then 'Male'
             else 'n/a'
        end cst_gender,
        cst_create_date
        from
         ( select *,ROW_NUMBER() OVER (PARTITION BY CST_ID order by cst_create_date desc ) as flag_last
        from bronze.crm_cust_info
        where cst_id is not null )t
        where flag_last =1
        set @end_time=GETDATE()
        print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

        -- erp_loc_A101 Load
        set @start_time=GETDATE()
        truncate  table silver.erp_loc_A101
        insert into silver.erp_loc_A101( cid, cntry )
        select
        replace(cid,'-','')cid,
        case when upper(trim(cntry))='DE' THEN 'GERMANY'
             when upper(trim(cntry)) in ('US','USA') THEN 'united states'
             when upper(trim(cntry)) ='' or cntry is null then 'n/a'
             else trim(cntry)
        end cntry
        from bronze.erp_loc_A101 
        set @end_time=GETDATE()
        print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

        -- erp_px_cat_g1v2 Load
        set @start_time=GETDATE()
        truncate  table silver.erp_px_cat_g1v2
        insert into silver.erp_px_cat_g1v2( id, cat, subcat, maintenance )
        select 
        id, cat, subcat, maintenance
        from bronze.erp_px_cat_g1v2
        set @end_time=GETDATE()
        print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

        -- erp_cust_AZ12 Load
        set @start_time=GETDATE()
        truncate  table silver.erp_cust_AZ12
        INSERT INTO silver.erp_cust_AZ12( cid, bdate, gen )
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
                ELSE cid 
            END AS cid,
            CASE 
                WHEN bdate > GETDATE() THEN NULL 
                ELSE bdate 
            END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_AZ12  
        set @end_time=GETDATE()
        print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

        -- crm_sales_details Load (The section causing the type clash if DDL isn't fixed)
        set @start_time=GETDATE()
        truncate  table silver.crm_sales_details
        insert into silver.crm_sales_details(
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_id, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price 
        )

        SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        case when sls_order_id =0 or len(sls_order_id) !=8 then null
             else cast(cast(sls_order_id as varchar)as date)
        end as sls_order_id, -- Alias must match target column name, outputs DATE
        case when sls_ship_dt =0 or len(sls_ship_dt) !=8 then null
             else cast(cast(sls_ship_dt as varchar)as date)
        end as sls_ship_dt,
        case when sls_due_dt =0 or len( sls_due_dt) !=8 then null
             else cast(cast( sls_due_dt as varchar)as date)
        end as  sls_due_dt,
        case  when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity *abs(sls_price)
              then sls_quantity *abs(sls_price)
            else sls_sales
        end as sls_sales,
        sls_quantity,
        case  when sls_price is null or sls_price <=0
              then sls_sales/ nullif(sls_quantity,0)
            else sls_price
        end as sls_price
        FROM warehouse.bronze.crm_sales_details
        set @end_time=GETDATE()
        print '----'+cast(datediff(second,@start_time,@end_time) as varchar)+'seconds';

        end try
    begin catch
	    print '------error occurec-----';
	    print 'error is '+error_message();
	end catch
End
GO

-- Batch 2: EXECUTION (Ensures procedure is created before being called)
exec silver.load_silver