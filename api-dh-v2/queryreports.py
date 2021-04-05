from utilsobjects import list2str

class QueryReports(object):
    def informacoesgerais(self,vendorcode,vendorclasscode,regioes,bandeira):
        strquery = """

            select 
                SUM(num_prods_total) as num_prods_total,SUM(num_prods) num_prods,
                round(SUM(num_prods)::decimal/SUM(num_prods_total)*100,2) percentualMeudesconto,
                min(num_clients_allocated) num_clients_allocated, sum(bought_offers) bought_offers,
                sum(activated_offers) activated_offers,
                min(first_wave_participated) first_wave_participated, max(last_wave_participated) last_wave_participated,
                max(num_waves_participated) num_waves_participated, max(last_wave_total) last_wave_total,
                vendor_class_code,vendor_code,
                vendor_class_name,vendor_name

            from md_dash_api_cube_no1
            where 1=1
                AND flag_period = '3_month' AND prod_hier_l30_desc = 'TOTAL CATEGORIAS'
                {vendor_class_code}
                {vendor_code}
                {regioes}
                {bandeira}
            group by vendor_class_code,vendor_code,vendor_class_name,vendor_name


        """
        bandeirastr  ,vendorclasscodestr , regioesstr , vendorcodestr = returnFilters(vendorcode,vendorclasscode,regioes,bandeira)
        

      

        result = strquery.format(vendor_class_code = vendorclasscodestr,vendor_code=vendorcodestr,regioes=regioesstr,bandeira=bandeirastr)
        return result
    
    def clientes(self,vendorcode,vendorclasscode,regioes,bandeira):
        strquery = """
            select           
                vendor_class_code,vendor_code,vendor_class_name,vendor_name               
                , max(total_store_category_clients) total_store_category_clients 
                , max(total_store_industry_clients) total_store_industry_clients 
                , max(md_category_clients) md_category_clients 
                , max(md_industry_clients) md_industry_clients  
                , max(total_store_industry_penetration) total_store_industry_penetration 
                , MAX(md_industry_penetration) md_industry_penetration 
            from md_dash_api_cube_no2
            where 1=1
                AND categoria = 'TOTAL CATEGORIA'
                {vendor_class_code}
                {vendor_code}
                {regioes}
                {bandeira}
                
            group by vendor_class_code,vendor_code,vendor_class_name,vendor_name

        """
   
        bandeirastr,vendorclasscodestr , regioesstr , vendorcodestr = returnFilters(vendorcode,vendorclasscode,regioes,bandeira)

        result = strquery.format(vendor_class_code = vendorclasscodestr,vendor_code=vendorcodestr,regioes=regioesstr,bandeira=bandeirastr)
        return result

    def IndicadoresPlataforma(self):
        strquery = """
            select           
                bandeira,
                activated_offers ofertas_ativadas,
                num_clients clientesComprando,
                sales 
            from md_dash_api_cube_no1
            where flag_period = '12_month'
            and REGION_CODE = 'TOTAL BRASIL'	
            and prod_hier_l30_desc = 'TOTAL CATEGORIAS'
        """
   
        

        result = strquery.format()
        return result

    def IndicadoresIndustria(self):
        strquery = """
        select           
                vendor_class_code,vendor_code,              
                bandeira,
                activated_offers ofertas_ativadas,
                num_clients  clientesComprando,
                bought_offers ofertasCompradas,
                sales vendas
              
            from md_dash_api_cube_no1
            where flag_period = '3_month'
            and REGION_CODE = 'TOTAL BRASIL'	
            and prod_hier_l30_desc = 'TOTAL CATEGORIAS'
            order by vendor_class_name,vendor_name

        """
   
        

        result = strquery.format()
        return result

    def vendasindustria(self,vendorcode,vendorclasscode,regioes,bandeira):
        strquery = """

            select          
                vendor_class_code,vendor_code,vendor_class_name,vendor_name,period
                ,sum(total_store_sales) total_store_sales
                ,sum(md_sales)  md_sales
            from md_dash_api_cube_no3
            where 1=1
                {vendor_class_code}
                {vendor_code}
                {regioes}
                {bandeira}
                and period <> 'TOTAL PERIODO'
                group by vendor_class_code,vendor_code,vendor_class_name,vendor_name,period
            order by period desc

        """
        
        bandeirastr  ,vendorclasscodestr , regioesstr , vendorcodestr = returnFilters(vendorcode,vendorclasscode,regioes,bandeira)


        result = strquery.format(vendor_class_code = vendorclasscodestr,vendor_code=vendorcodestr,regioes=regioesstr,bandeira=bandeirastr)
        return result

    def ultimosciclos(self,vendorcode,vendorclasscode,regioes,bandeira):


        strquery = """

            select          
                vendor_class_code,vendor_code,vendor_class_name,vendor_name,wave               
                ,sum(md_sales)  md_sales
            from md_dash_api_cube_no4
            where 1=1 and
                source = 'last 5'
                {vendor_class_code}
                {vendor_code}
                {regioes}
                {bandeira}
            group by vendor_class_code,vendor_code,vendor_class_name,vendor_name,wave,ranking 
           
            order by ranking desc
            LIMIT 5

        """
        
        bandeirastr  ,vendorclasscodestr , regioesstr , vendorcodestr = returnFilters(vendorcode,vendorclasscode,regioes,bandeira)


        if 'TOTAL BANDEIRAS' in bandeirastr:
            bandeirastr = ''
            
        result = strquery.format(vendor_class_code = vendorclasscodestr,vendor_code=vendorcodestr,regioes=regioesstr,bandeira=bandeirastr)
        return result

    def topciclos(self,vendorcode,vendorclasscode,regioes,bandeira):

        strquery = """

            select          
                vendor_class_code,vendor_code,vendor_class_name,vendor_name,wave
                ,sum(md_sales)  md_sales
            from md_dash_api_cube_no4
            where 1=1 and
                source = 'top 5'
                {vendor_class_code}
                {vendor_code}
                {regioes}
                {bandeira}
            group by vendor_class_code,vendor_code,vendor_class_name,vendor_name,wave,ranking 
            order by ranking desc
            LIMIT 5
        """
        
        bandeirastr, vendorclasscodestr,regioesstr,vendorcodestr = returnFilters(vendorcode,vendorclasscode,regioes,bandeira)

        if 'TOTAL BANDEIRAS' in bandeirastr:
            bandeirastr = ''

        result = strquery.format(vendor_class_code = vendorclasscodestr,vendor_code=vendorcodestr,regioes=regioesstr,bandeira=bandeirastr)
        return result


def returnFilters(vendorcode,vendorclasscode,regioes,bandeira):

    bandeirastr = ""
    vendorclasscodestr =""
    regioesstr = ""
    vendorcodestr =""

    if len(bandeira) >0: 
        if bandeira =='PA' or  bandeira =='CLEX':
            bandeirastr = " and bandeira = '{}'".format(bandeira)
        else:
            bandeirastr = " and bandeira = 'TOTAL BANDEIRAS'"
    else:
        bandeirastr = " and bandeira = 'TOTAL BANDEIRAS'"

    if len(regioes)>=0:            
        regioesstr = " and region_code in ({})".format(list2str(regioes))
    else:
        regioesstr = " and region_code in ('TOTAL BRASIL')"

    if vendorclasscode !='':
        vendorclasscodestr = " and vendor_class_code='{}'".format(vendorclasscode)
    if vendorcode != '':
        vendorcodestr = " and vendor_code='{}'".format(vendorcode)

    return bandeirastr,regioesstr,vendorclasscodestr,vendorcodestr
