class Queries():
    def innerPersonas(self):

        return "inner join personas on u.prsn_id = personas.prsn_id {filter_personas}"

    def Lealdade50(self):
        #Lealdade 1        
        strQuery = """
                select count(*) 
                from t_audience_l21_{l21} 
                    {joins} 
                where 
                    prod_code in ({prod_code}) 
                    and is_loyal_a = {loyal} 
                    {filters} 
                    {program} 
                    {regions}    
                """
        return get_query_and_table(strQuery)
    
    def getProductInfo(self):
        #Lealdade 1        
        strQuery = """
                SELECT prod_hier_l10_code FROM product_structure where prod_code = '{}';   
                """
        return  strQuery

       
    def LealdadeBuysL20NotProd(self):   
        #except     
        strQuery = """
                Select

                (select count(*) 
                from un_filters 
                    {joins_a} 
                where 
                    un_{un_code} = 1 
                    {condition} {filters} {program} {regions}  ) -   
                
                (SELECT coalesce(count(distinct t_audience_l21_{str_l21}.prsn_id),0) as audience 
                FROM t_audience_l21_{str_l21} {joins_b}                                   
                WHERE prod_code in ({prod_code_list})

                {objective} {filters} {program} {regions} )
                """
                 
        return strQuery,'un_filters','t_audience_l21_{str_l21}'


    def LealdadeDoesntBuyl20(self):        
        strQuery = """
                    select count (*)
                        from un_filters {joins} 
                        where un_{un_code} = 0 
                        {condition}
                        {filters} {program} {regions}
                """
        return get_query_and_table(strQuery)


    def LealdadeDoesntBuyProductNoUN(self): 
        """
            Lealdade 5
        """
        # usar Except       
        strQuery = """
                    SELECT (
                            (select count(*) from un_filters where {filters_un} {loyalty_program} {regions})
                            -
                            ({audience})
                         ) as c
                """
        return get_query_and_table(strQuery)

    def LealdadeDoesntBuyProductAudience(self): 
        """
            Lealdade 5
        """
        # usar Except       
        strQuery = """
                    select u.prsn_id from t_audience_l21_{l30} u
                    inner join  
                         product_filter prd on prd.prod_code = u.prod_code
                    where 1=1 
                            {filters} {program} {regions}                               
                """
        return get_query_and_table(strQuery)
    
                        
    

    def LealdadeDoesntBuyProductUN(self):
        #Lealdade 5
        #         
        strQuery = """
                    SELECT count(distinct(u.prsn_id))
                          FROM un_filters u
                          {join_persona}
                          LEFT JOIN 
                          
                            ({audience}) aud
                          
                          ON (u.prsn_id = aud.prsn_id)
                          where aud.prsn_id is NULL
                            {condition}
                            {filters_un_table}
                            {loyalty_program_code_un_table}
                            {regions_un_table}
                """
        return get_query_and_table(strQuery)
    

    def LealdadeSpendMostMyProductLoyalty10(self):
        #lealdade 6
        strQuery = """
                   Select 
                        u.prsn_id
                    from t_audience_l21_{l30} u
                        {joins}
                        inner join  
                         product_filter prd on prd.prod_code = u.prod_code
                        where is_recovery_candidate = false 
                        {filters} {program} {regions}
                """
        return get_query_and_table(strQuery)
    def LealdadeSpendMostMyProduct(self):
        #lealdade 6
        strQuery = """
                   Select 
                        u.prsn_id
                    from t_audience_l21_{l30} u
                        {joins}
                        inner join  
                         product_filter prd on prd.prod_code = u.prod_code
                        where is_recovery_candidate = false and
                        {loyalty} = {loyal} {filters} {program} {regions}
                """
        return get_query_and_table(strQuery)

    def LealdadeLaunchProduct(self):
        #lealdade 10
        strQuery = """
                   
                   SELECT 
                        SUM(cast(prsn_qty as bigint)) qty 
                    from prod_new_prsn_count_filter 
                    where 
                            prod_comml_code in ('{lcode}') {filters} {program} {regions}
                """
        return get_query_and_table(strQuery)

    def LealdadeRecoverProduct(self):
        #lealdade 20
        strQuery = """
                  Select u.prsn_id
                  from t_audience_l21_{l30}  u {joins}
                  inner join product_filter prd on prd.prod_code = u.prod_code
                  where 1=1
                        and is_recovery_candidate = {loyal} {filters} {program} {regions}
                """
        return get_query_and_table(strQuery)

    def  IndexInfoProd(self):
        strQuery = """ 
            
            SELECT 
                 AVG(l20_units_per_visit::numeric) as l20_units_per_visit, min(min_perc_disc) as min_perc_disc, 
                 bool_or(sold_by_weight_flag) as sold_by_weight_flag,bool_or(is_alcohol) as is_alcohol
                
            from                  
                product_filter prd 
           
           
        """
        return strQuery

    def  IndexInfoL20(self):
        strQuery = """ 
            
            SELECT 
                 AVG(l20_units_per_visit::numeric) as l20_units_per_visit, min(min_perc_disc) as min_perc_disc, 
                 bool_or(sold_by_weight_flag) as sold_by_weight_flag,bool_or(is_alcohol) as is_alcohol,
                 , min(prod_un_code) prod_un_code
            from 
                product_structure ind inner join
                (select distinct (l20_code)  from product_filter) prd on ind.prod_hier_l20_code = l20_code 
           
           
        """
        return strQuery

    def conversion_rates(self):
        strQuery ="""
            select coalesce(AVG(conversion_rate),0) as conversion_rate from conversion_rates cr  
            inner join product_filter pr on {level}_code = lxx_code 
            where
                level_code = '{level}'
                
                
            """
        return strQuery


    def ProductStructure(self):
        strQuery = """
            CREATE TEMP TABLE product_filter AS
            SELECT prod_code, prod_hier_l10_code l10_code,prod_hier_l20_code l20_code,prod_hier_l30_code l30_code,prod_hier_l40_code l40_code,avg_prod_price_clex, 
                    avg_prod_price_pa,sold_by_weight_flag,l20_units_per_visit,is_alcohol,min_perc_disc
            FROM 
                product_structure ps
            WHERE 
                {wheres}
                {vendor}
            ;

            SELECT distinct l20_code, l30_code
            FROM 
                product_filter;

        """
        return strQuery
    
    def ProductStructureFamily(self):
        strQuery = """
            CREATE TEMP TABLE product_filter AS
             
            SELECT distinct ps.prod_code,  
                ps.prod_hier_l10_code l10_code,ps.prod_hier_l20_code l20_code,
                ps.prod_hier_l30_code l30_code,ps.prod_hier_l40_code l40_code,
                ps.supplier_code,ps.avg_prod_price_clex,ps.avg_prod_price_pa,ps.sold_by_weight_flag,
                ps.l20_units_per_visit,ps.is_alcohol,ps.min_perc_disc
            FROM 
                product_structure ps 
                    
                
            WHERE 
               ps.prod_hier_l10_code ='{subgrupo}' and ps.family_code = '{family}'
            {vendor}
            ;

            SELECT distinct l20_code, l30_code
            FROM 
                product_filter;

        """
        return strQuery
    


    def AveragePrice(self):
        strQuery = """
            
			select (
            SELECT  coalesce(AVG(avg_prod_price_clex),0) avg_prod_price_clex                   
            FROM product_filter  where avg_prod_price_clex > 0) as avg_prod_price_clex,
            
            (select coalesce(AVG(avg_prod_price_pa),0) avg_prod_price_pa  
            FROM product_filter  where avg_prod_price_pa > 0) as avg_prod_price_pa
                
                     
        """
        return strQuery
    def TableExists(self):
        strQuery = """
            
            SELECT to_regclass('{}') as  table;
            
           
        """
        return strQuery
            
         
           
def get_query_and_table(query):
    """
            Returns the correspondent query and the main table name
    """
    

    # By the FROM key-word we get the respective table name
    table = query[query.upper().index(' FROM ') + 6 : ]
    table = table.split(' ')[0]

    return query, table