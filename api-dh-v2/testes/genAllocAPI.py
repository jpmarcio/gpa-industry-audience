#coding: utf-8

#import the standard functiosn/defs
import numpy as np
import pandas as pd
import os
import datetime 
from sqlalchemy.types import NVARCHAR
import calendar
import datetime,time
import pandas as pd
pd.options.display.max_columns = None
from datetime import datetime, date, timedelta
from pyspark.sql.window import Window
import gcsfs
import time
import datetime
from datetime import timedelta

#importing pyspark sutff
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, asc
import pyspark.sql.functions as F
from pyspark.sql.functions import broadcast
from cap_tools.notify.utils import make_simple_request 


#importing pyspark sutff
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, asc
import pyspark.sql.functions as F
from pyspark.sql.functions import broadcast
from cap_tools.notify.utils import make_simple_request    
import time
import datetime
from datetime import timedelta

from dhutilsMD import *
#import utils_activation as utils_activation



class genAllocAPI:
    """At this class we run a small allocation process to deliver lists of prsn_codes
    for each Survey ordered from CPGs. The filters available for survey audiences
    are the same as for industry allocation
    """
    def __init__(self, env):
        
        self = dhInit(self, env)
        
        # ****** Some treatments for IDE to lessen the qty of errors
        self.host = ''
        #self.spark = {}
        self.imports = {}
        self.exports = {}
        self.dna = {}
        self.storageFormat = ''
        #self.timeZone = ''
        self.sc = None
        # ******
        self.file_id = None
        self.filename = None
        
        self.project_shortname = 'md_dash_api5'


        self.start_date = '2020-04-01'
        self.end_date = '2020-06-30'

        #period 2
        self.start_date2 = '2020-04-01'
        self.end_date2 = '2020-06-30'

        #period 3
        self.start_date3 = '2019-07-01'
        self.end_date3 = '2020-06-30'
        
        
        self.start_date = '2020-09-17'
        self.end_date = '2020-11-26'

        #period 2
        self.start_date2 = '2020-09-17'
        self.end_date2 = '2020-11-26'

        #period 3
        self.start_date3 = '2020-01-09'
        self.end_date3 = '2020-11-26'
                
                
        
        
        
    def logTeams(self, msg):
        teams_url = self.teams_url
        make_simple_request(teams_url, msg)

        print(msg)
        
        
        
        
    def createCube(self, l, table_name, metrics, regular_select, regular_grp_by): 
    
        from itertools import combinations
        input_select = []
        for i in range(0, len(l)):
            input_select.append(l[i][0]+" as "+l[i][1])

        input = []
        for i in range(0, len(l)):
            input.append(l[i][0])

        aux = []
        for i in range(0, len(l)):
            aux.append(l[i][1])

        l2 = [[i, j] for i,j in l]
        for i in range(0, len(l2)):
            novo = l2[i][0][1:len(l2[i][0])-1]
            l2[i][0] = novo
            l2[i] = tuple(l2[i])



        output_slt = sum([list(map(list, combinations(input_select, i))) for i in range(len(input_select) + 1)], [])
        output = sum([list(map(list, combinations(input, i))) for i in range(len(input) + 1)], [])



        # In[3]:


        cubo = ([])

        for x in range(0, len(output)):

            cubo.append(output[x]+aux)


        cubo_grp_by = ([])

        for x in range(0, len(output)):

            cubo_grp_by.append(output_slt[x]+aux)


        # In[4]:


        #primeiro ele anda em cada elemento da combinacao
        for x in range(0, len(cubo)):
            try:
                #acessa o primeiro elemento
                for y in range(0,len(cubo[x])): 
                    aux_as = 0
                    aspas = 0
                    for w in range(0,len(cubo_grp_by[x][y])):
                        if(cubo_grp_by[x][y][w] == "'"):
                            aspas = aspas + 1
                            if(aspas == 2): 
                                aux_as = aux_as + w
                                #print(cubo_grp_by[x][y][1:aux_as])

                    #verifica se tem alguma duplicata
                    for t in range(0,len(l)):

                        if(cubo[x][y]== l[t][0] and l[t][1] in cubo[x]):
                            #troca de lugar com o original para ficar na order
                            aux=cubo[x].index(l[t][1])
                            cubo[x][y],cubo[x][aux] = l[t][1],cubo[x][y]
                            #remove um e deixa somente o cubo
                            cubo[x].remove(l[t][1])


            except:
                continue



        # In[5]:


        #primeiro ele anda em cada elemento da combinacao
        for a in range(0,3):
            for x in range(0, len(cubo_grp_by)):
                try:
                    #acessa o primeiro elemento
                    for y in range(0,len(cubo_grp_by[x])): 
                        aux_as = 0
                        aspas = 0
                        for w in range(0,len(cubo_grp_by[x][y])):
                            if(cubo_grp_by[x][y][w] == "'"):
                                aspas = aspas + 1
                                if(aspas == 2): 
                                    aux_as = aux_as + w
                                    #print(cubo_grp_by[x][y][1:aux_as])


                        #verifica se tem alguma duplicata
                        for t in range(0,len(l)):

                            if(cubo_grp_by[x][y][1:aux_as] == l2[t][0] and l2[t][1] in cubo_grp_by[x]):
                                #troca de lugar com o original para ficar na order
                                #print("tem igual:"+l2[t][1])
                                aux=cubo_grp_by[x].index(l2[t][1])
                                cubo_grp_by[x][y],cubo_grp_by[x][aux] = l2[t][1],cubo_grp_by[x][y]
                                #remove um e deixa somente o cubo
                                cubo_grp_by[x].remove(l2[t][1])


                except:
                    continue
        slct_g = ([])
        for w in range(0, len(cubo)):
                slct_g.append(" Group by "+cubo[w][0])
                for q in range(1,len(cubo[w])):
                    aux = cubo[w][q] 
                    slct_g.append("," + aux)
                #print(slct_g)


        # In[36]:


        gpb_by = ([])
        for x in range(0, len(cubo)):
            slct = regular_grp_by+ " "+cubo[x][0]
            for y in range(1, len(cubo[x])):
                #slct = slct + "," + cubo[x][y] 
                slct = slct + "," + cubo[x][y] 

            slct = slct 
            gpb_by.append(slct)

            #print(slct)




        # In[37]:


        select = ([])
        for x in range(0, len(cubo_grp_by)):
            slct = regular_select+ " "+cubo_grp_by[x][0]
            for y in range(1, len(cubo_grp_by[x])):
                #slct = slct + "," + cubo_grp_by[x][y] 
                slct = slct + "," + cubo_grp_by[x][y] 

            slct = slct + "," +metrics + " from "+ table_name
            select.append(slct)

            print(slct)

        output = []
        sql = select[0]+" "+gpb_by[0]
        print(sql)
        #output = sql
        output = execSql(self,sql)

        print(output)

        for i in range(1,len(select)):
            sql=select[i]+" "+gpb_by[i]
            print("Tabela:")
            print(sql)
            tabela = execSql(self,sql)
            output = output.unionAll(tabela)
        print(output)
        return(output)

    print('imported')



    def find_date_filters(self): 
        """It finds out the start/end date period of analysis for each DNA 
        (is its 52 weeks? 104 week? 6 months?). This is based in the json file input for 'period'."""

        self.fis_week_id_start = execSql(self,"select fis_week_id from ci_brazil_easl.date_dim dt where dt.date_id = '{date}'".format(date=self.start_date)).rdd.flatMap(list).first()         
        self.fis_week_id_end = execSql(self,"select fis_week_id from ci_brazil_easl.date_dim dt where dt.date_id = '{date}'".format(date=self.end_date)).rdd.flatMap(list).first()
        self.fis_year_id_start = str(self.fis_week_id_start)[0:4]
        self.fis_year_id_end = str(self.fis_week_id_end)[0:4]



    
    def create_allocation_store_table(self):

        preferred_store = self.sqlContext.table('ci_brazil_md.md_features_preferred_store_lkp').select('*')
        store_dim_c = self.sqlContext.table('ci_brazil_easl.store_dim_c').select('store_id','store_code','store_name','banner_code','store_mgmt_l20_desc','store_mgmt_l20_code')
        store = preferred_store.join(store_dim_c, [preferred_store.preferredStore==store_dim_c.store_code], 'inner')
        execSql(self, "drop table if exists {tmpdb}.{prj_shortname}_str2".format(tmpdb=self.tmpdb,prj_shortname=self.project_shortname))
        store.write.mode("overwrite").saveAsTable("{tmpdb}.{prj_shortname}_str2".format(tmpdb=self.tmpdb,prj_shortname=self.project_shortname))




    def create_offerbank_product_table(self):

        #prod creation - I do this to get vendor informations
        prod_offerbank = self.sqlContext.table('ci_brazil_md.md_offerbank_product_h').select('idt_promo','idt_plu','start_date','ind_prod_principal').where("start_date >= '2020-04-01'")
        prod_dim_c = self.sqlContext.table('ci_brazil_easl.prod_dim_c').select('prod_code','vendor_name','vendor_code','vendor_class_name','vendor_class_code','prod_hier_l30_desc')
        prod = prod_offerbank.join(prod_dim_c, [prod_offerbank.idt_plu == prod_dim_c.prod_code], 'inner')
        prod = prod.select('idt_promo','start_date','vendor_name','vendor_code','vendor_class_name','vendor_class_code','prod_hier_l30_desc','ind_prod_principal')
        #prod = prod.where("ind_prod_principal = 'S'")
        prod = prod.dropDuplicates()
        prod.write.mode("overwrite").saveAsTable("{tmpdb}.{project_shortname}_prod3".format(tmpdb=self.tmpdb, project_shortname = self.project_shortname))
        
    


    def create_summary_alloc_cubes(self, regular_select,regular_grp_by, where, cubo_name):  
    #sqlContext.sql("""drop table {ci_brazil_analyst_tmp}.{prj_shortname}_total_waves_summary""".format(ci_brazil_analyst_tmp=parameters['tmp_table_bucket'],prj_shortname=parameters['project_shortname']))


        print("entrou")
        print("regular_select: "+regular_select)
        print("regular_grp_by: "+regular_grp_by)
        print("where: "+where)

        cube_code = [  ("'TOTAL BRASIL'","store_mgmt_l20_code")
        ,("'TOTAL BANDEIRAS'","fidelityprogcode")]


        metrics = """
        count(distinct prsn_id) as allo_cli
        """

        ##########basic cubes##############################################

        #-------------total store category data (table name: total_category_trans_str)-------------------------

        from_ = """(
                select alloc.*, pd.*, str.store_mgmt_l20_code as store_mgmt_l20_code
                from ci_brazil_md.md_measurement_allocation_base alloc
                inner join {tmpdb}.{project_shortname}_str2 str on str.prsn_id = alloc.prsn_id
                inner join {tmpdb}.{project_shortname}_prod3 pd on pd.start_date = alloc.start_date and pd.idt_promo = alloc.promo_id 
                {where}
                )
            """.format(where=where, tmpdb = self.tmpdb, project_shortname = self.project_shortname)

        cube_result1 = self.createCube(cube_code, from_, metrics, regular_select, regular_grp_by)
        cube_result1.write.mode("overwrite").saveAsTable("{ci_brazil_analyst_tmp}.{prj_shortname}_final_alloc_new_{cubo_name}".format(ci_brazil_analyst_tmp=self.tmpdb,prj_shortname=self.project_shortname,cubo_name=cubo_name))
        
        #return cube_result1


    def create_alloc_md_total_table_loop(self):


        vendor_list = self.sqlContext.sql("select distinct vendor_code from {ci_brazil_analyst_tmp}.{prj_shortname}_activation_offerbank ".format(ci_brazil_analyst_tmp=self.tmpdb,prj_shortname=self.project_shortname)).rdd.flatMap(lambda x: x).collect()
        
        print ("cube1")
        cubo_name = "cube1"
       

        regular_select = """select vendor_code, vendor_class_code,"""
        regular_grp_by = """group by vendor_code, vendor_class_code, """
        where = """"""
        self.create_summary_alloc_cubes_loop(regular_select,regular_grp_by, where, cubo_name, vendor_list)







    def create_summary_alloc_cubes_loop(self, regular_select,regular_grp_by, where, cubo_name, vendor_list):  
        #sqlContext.sql("""drop table {ci_brazil_analyst_tmp}.{prj_shortname}_total_waves_summary""".format(ci_brazil_analyst_tmp=parameters['tmp_table_bucket'],prj_shortname=parameters['project_shortname']))


        last_wave = self.sqlContext.sql("select max(start_date) from ci_brazil_md.md_measurement_transactional_base limit 1").rdd.flatMap(list).first()
        x_new = last_wave
        x_new - timedelta(days=70)
        last_date = x_new.strftime("%Y-%m-%d")
        start_3month = (x_new - timedelta(days=70)).strftime("%Y-%m-%d")
        start_12month = (x_new - timedelta(days=350)).strftime("%Y-%m-%d")


        loop = 0
        acumulator = 500

        vendor_list_done = ['']

        overwrite = True

        while len(vendor_list_done) < len(vendor_list):
            
            print("done: "+str(len(vendor_list_done)))
            print("missing: "+str(len(vendor_list)))
        
        
            vendor_list_to_run = vendor_list[loop:loop+acumulator]
            
            print(str(loop)+":"+str(loop+acumulator))
            
            print("Extracting allocation of client: "+str(vendor_list_to_run))

            print("entrou")
            print("regular_select: "+regular_select)
            print("regular_grp_by: "+regular_grp_by)
            print("where: "+where)

            cube_code = [  ("'TOTAL BRASIL'","preferredStore_mgmt_l20_desc")
            ,("'TOTAL BANDEIRAS'","fidelityprogcode")]


            metrics = """
            count(distinct alo.prsn_id) as total_prsn_allocated
            ,count(distinct case when alo.start_date = '{last_date}' then alo.prsn_id else null end) as prsn_allocated_last_wave
            ,count(distinct case when alo.start_date between '{start_3month}' and '{last_date}' then alo.prsn_id else null end) as prsn_allocated_last_6_waves
            ,count(distinct case when alo.start_date between '{start_12month}' and '{last_date}' then alo.prsn_id else null end) as prsn_allocated_last_26_waves

            """.format(last_date=last_date,start_3month=start_3month,start_12month=start_12month)

            ##########basic cubes##############################################

            #-------------total store category data (table name: total_category_trans_str)-------------------------

            from_ = """(
                    select *, alo.banner as fidelityprogcode
                    from ci_brazil_md.md_allocation_h alo
                    left join ci_brazil_md.md_api_promo_prod2 pd on pd.idt_promo = alo.promo_id and alo.start_date = pd.start_date
                    left join ci_brazil_md.md_features_preferred_store_lkp str on str.banner = alo.banner and alo.prsn_id = str.prsn_id
                    where alo.start_date >= '{start_12month}'
                    and vendor_code in ({vendor})
                    )
                """.format(where=where, tmpdb = self.tmpdb, project_shortname = self.project_shortname,last_date=last_date,start_3month=start_3month,start_12month=start_12month,vendor=str(vendor_list_to_run)[1:-1])

            cube_result1 = self.createCube(cube_code, from_, metrics, regular_select, regular_grp_by)
            #cube_result1.write.mode("overwrite").saveAsTable("{ci_brazil_analyst_tmp}.{prj_shortname}_final_alloc_new_{cubo_name}".format(ci_brazil_analyst_tmp=self.tmpdb,prj_shortname=self.project_shortname,cubo_name=cubo_name))
        
    
            cube_result1.write.format("parquet") \
            .mode("overwrite" if overwrite else "append") \
            .saveAsTable("ci_brazil_md.md_measurement_allocation_industryloop")
            
            overwrite = False
            
            loop += acumulator 
            print("Loop: "+str(loop))
            
            vendor_list_done = vendor_list_done + vendor_list_to_run
    

            #return cube_result1



    def create_alloc_md_total_table(self):
        
        print ("cube1")
        cubo_name = "cube1"
        regular_select = """select 1 as dummy,'total' as period,vendor_name, vendor_code, vendor_class_name, vendor_class_code, 'TOTAL CATEGORIAS' as prod_hier_l30_desc,"""
        regular_grp_by = """group by 1,'total',vendor_name, vendor_code, vendor_class_name, vendor_class_code, 'TOTAL CATEGORIAS', """
        where = """"""
        self.create_summary_alloc_cubes(regular_select,regular_grp_by, where, cubo_name)

        print ("cube2")
        cubo_name = "cube2"
        regular_select = """select 1 as dummy,'3_month' as period,vendor_name, vendor_code, vendor_class_name, vendor_class_code, 'TOTAL CATEGORIAS' as prod_hier_l30_desc,"""
        regular_grp_by = """group by 1,'3_month',vendor_name, vendor_code, vendor_class_name, vendor_class_code,'TOTAL CATEGORIAS',"""
        where = """where alloc.start_date between '{start_date2}' and '{end_date2}'""".format(start_date=self.start_date,end_date=self.end_date
            ,start_date2=self.start_date2,end_date2=self.end_date2
            ,start_date3=self.start_date3,end_date3=self.end_date3)
        self.create_summary_alloc_cubes(regular_select,regular_grp_by, where, cubo_name)
        
        print ("cube3")
        cubo_name = "cube3"
        regular_select = """select 1 as dummy,'12_month' as period,'total' as vendor_name, 'total' as vendor_code, 'total' as vendor_class_name, 'total' as vendor_class_code, 'TOTAL CATEGORIAS' as prod_hier_l30_desc,"""
        regular_grp_by = """group by 1,'12_month','total', 'total', 'total', 'total','TOTAL CATEGORIAS',"""
        where = """where alloc.start_date between '{start_date3}' and '{end_date3}'""".format(start_date=self.start_date,end_date=self.end_date
            ,start_date2=self.start_date2,end_date2=self.end_date2
            ,start_date3=self.start_date3,end_date3=self.end_date3)
        self.create_summary_alloc_cubes(regular_select,regular_grp_by, where, cubo_name)
        
        final_cube = cube1.union(cube2)
        final_cube = final_cube.union(cube3)
        
        self.sqlContext.sql("drop table if exists {ci_brazil_analyst_tmp}.{prj_shortname}_final_alloc_total_period_cat2".format(ci_brazil_analyst_tmp=self.tmpdb,prj_shortname=self.project_shortname))
        final_cube.write.mode("overwrite").saveAsTable("{ci_brazil_analyst_tmp}.{prj_shortname}_final_alloc_total_period_cat2".format(ci_brazil_analyst_tmp=self.tmpdb,prj_shortname=self.project_shortname))
        

                
    


    def do_cubes(self):

        """ This do_dna_teste def is the caller of the other defs and processes. 
        It loops each DNA and DATE that needs to be processed and extracts all the needed KPIS"""

        self.logTeams(f"**Starting API CUBES**")

        

        #1 - Creates tables that are shared through all the cubes 
        self.find_date_filters()

        self.create_offerbank_product_table()
        self.create_allocation_store_table()

        #2 - Creates tables that are auxiliar to cubes 
        self.create_alloc_md_total_table_loop()
        
            
        self.logTeams(f"**Finished API alloc info segmentation**")
                    
        



# This module has a class, but can be self executed as well!!
if __name__ == '__main__':
    # create MD env
    name = 'cubealoc'
    dummy, env = initialise_md_env(mode='dev', run_name=name)
    self=dummy

    env['sc'].setLogLevel("ERROR")

    print("Running Spark version:\nConfigured: {c}\nDetected: {d}".format(c=dummy.spark['version'],d=env['sc'].version))


    # Start the genDNASegmentation class and call the main method
    galloc = genAllocAPI(env)
    galloc.do_cubes()




