import json
import sys
from createfilter import GetFilter
from testes.datatest import *
from meudesconto import execute_query, execute_query_temp
import pandas as pd

def test_padrao():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 6
        modalidade = data_Sql_filterIndustriaIModalidade
        obj = GetFilter('INDUSTRIA').get(filters[0])     
        print(obj)
        assert True
    except Exception as e:          
        assert False,repr(e)

def _test_Sql_filterIndustriaLealdade1():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 6
        modalidade = data_Sql_filterIndustriaIModalidade
        obj = GetFilter('INDUSTRIA').get(filters[0])         
        ret = obj.get_sql_filters(modalidade).replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        assert ret == ' '.join(data_Sql_filterIndustria_lealdade1_response.split()).lower(),'SQL command wasnt built properly'
        execute_query(ret)
    except Exception as e:          
        assert False,repr(e)


def _test_Sql_filterIndustriaLealdade2():
    try:
        filters = data_Sql
        filters[0]['filtros']['lealdadeProduto'] = 2
        index_info = data_Sql_filterIndustriaIndexInfo
        obj = GetFilter('INDUSTRIA').get(filters[0])        
        ret = obj.get_sql_filters(index_info).replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        assert ret == ' '.join(data_Sql_filterIndustria_lealdade2_response.split()).lower(),'SQL command wasnt built properly'
        execute_query(ret)
    except Exception as e:          
        assert False,repr(e)

def _test_Sql_filterIndustriaLealdade3():
    try:
        filters = data_Sql
        filters[0]['filtros']['lealdadeProduto'] = 3
        index_info = data_Sql_filterIndustriaIndexInfo
        obj = GetFilter('INDUSTRIA').get(filters[0])        
        ret = obj.get_sql_filters(index_info).replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        assert ret == ' '.join(data_Sql_filterIndustria_lealdade3_response.split()).lower(),'SQL command wasnt built properly'
        execute_query(ret)
    except Exception as e:          
        assert False,repr(e)

def _test_Sql_filterIndustriaLealdade4():
    try:
        filters = data_Sql
        filters[0]['filtros']['lealdadeProduto'] = 4
        index_info = data_Sql_filterIndustriaIndexInfo
        obj = GetFilter('INDUSTRIA').get(filters[0])        



        ret = obj.get_sql_filters(index_info).replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        assert ret == ' '.join(data_Sql_filterIndustria_lealdade4_response.split()).lower(),'SQL command wasnt built properly'
        execute_query(ret)
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade5():
    try:
    
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 5        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        modalidade  = filters[0]['modalidade']

        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        assert ret == ' '.join(data_Sql_filterIndustria_lealdade5_response.split()).lower(),'SQL command wasnt built properly'
        execute_query_temp(ret,conn)
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade5_UN():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 5        
        filters[0]['filtros']['un'] = [{
            "codigo": "01011403034002",
            "descricao": "SHAMPOO BASICO"
            },
            {
            "codigo": "01011403040",
            "descricao": "HIDRATACAO CABELO"
        }]
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
     
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        assert ret == ' '.join(data_Sql_filterIndustria_lealdade5_un_response.split()).lower(),'SQL command wasnt built properly'
        execute_query_temp(ret,conn)
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade6():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 6        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        modalidade  = filters[0]['modalidade']


        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        assert ret == ' '.join(data_Sql_filterIndustria_lealdade6_response.split()).lower(),'SQL command wasnt built properly'
        execute_query_temp(ret,conn)
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade7():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 7        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        modalidade  = filters[0]['modalidade']

     

        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        assert ret == ' '.join(data_Sql_filterIndustria_lealdade7_response.split()).lower(),'SQL command wasnt built properly'
        execute_query_temp(ret,conn)
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade10():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 10        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        modalidade  = filters[0]['modalidade']

     
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        assert ret == ' '.join(data_Sql_filterIndustria_lealdade10_response.split()).lower(),'SQL command wasnt built properly'
        execute_query_temp(ret,conn)
        conn.close()
    except Exception as e:          
        assert False,repr(e)
   
def test_Sql_filterIndustriaLealdade20():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 20       
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        modalidade  = filters[0]['modalidade']

  
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)
        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        assert ret == ' '.join(data_Sql_filterIndustria_lealdade20_response.split()).lower(),'SQL command wasnt built properly'
        execute_query_temp(ret,conn)
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def getSql_filters(obj):
    productsql = obj.getSqlProductTemporaryTable()

    result,conn = execute_query_temp(productsql,None)
    # create a temp table with products and return a list with l21 
    # each l30 in a diferent audience table
    result = pd.DataFrame(result, columns=['l20_code', 'l30_code'])
    l20 = result['l20_code']
    l30 = result['l30_code'].unique()

    if len(l30)==0:
            raise Exception("Nao foram encontrados produtos(cubos) para esta selecao")

    obj.l30 = l30


    ret,level_conversion = obj.get_sql_filters()   
    
    return ret,conn
