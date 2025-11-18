---gold layer ..
create view gold.dim_customers as 
SELECT 
    --creating a unique key using surrogate key
       ROW_NUMBER() over (order by cst_id) as customer_key ,
       ci.cst_id as customer_id,
       ci.cst_key as customer_number,
       ci.cst_firstname as first_name,
       ci.cst_lastname as last_name,
       ci.cst_marital_status as marital_status,
       la.cntry as country,
      case when ci.cst_gender !='n/a' then ci.cst_gender
            else coalesce(ca.gen,'n/a')
       end as gender,
       ca.bdate as birthdate,
       ci.cst_create_date as create_date  
  from silver.crm_cust_info ci
 left join silver.erp_cust_AZ12 ca
 on ci.cst_key=ca.cid
 left join silver.erp_loc_A101 la
 on ci.cst_key=la.cid

 --------------------product view----------------------
 create view gold.dim_product as 
 SELECT 
       ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key ,
      pn.prd_id as product_id,
      pn.prd_key as product_number,
      pn.prd_nm as product_name,
      pn.prd_cost as cost,
      pn.prd_line as product_line,
      pn.prd_start_dt as start_date,
      pn.cat_id as category_id,
      pc.cat as category,
      pc.subcat as subcategory,
      pc.maintenance 
  FROM silver.crm_prd_info pn
  left join silver.erp_px_cat_g1v2 pc
  on pn.cat_id=pc.id
  where prd_end_dt is null

  -----------------------------------sales view------------
 create view gold.fact_sales as 
SELECT
       sd.sls_ord_num as order_number
      ,pr.product_key 
      ,cu.customer_key
      ,sd.sls_order_id as order_date
      ,sd.sls_ship_dt as shipping_date
      ,sd.sls_due_dt  as due_date
      ,sd.sls_sales  as sales_amount
      ,sd.sls_quantity as quantity
      ,sd.sls_price as price
  FROM silver.crm_sales_details sd
  left join gold.dim_product pr 
  on sd.sls_prd_key=pr.product_number
  left join gold.dim_customers cu
  on sd.sls_cust_id=cu.customer_id
