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




def test_Sql_filterIndustriaLealdade6_plu1072870():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 6  
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['1072870']
        filters[0]['modalidade']['subgrupo'] = '004'
        filters[0]['modalidade']['grupo'] = '565'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0303'
        filters[0]['modalidade']['vendor_code'] = '0000000059021'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":6, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['BA','CE','GO','MG','MS','PB','PE','PI','PR','RJ','SE']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==7
        conn.close()
    except Exception as e:          
        assert False,repr(e)


def test_Sql_filterIndustriaLealdade7_plu1072870():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['1072870']
        filters[0]['modalidade']['subgrupo'] = '004'
        filters[0]['modalidade']['grupo'] = '565'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0303'
        filters[0]['modalidade']['vendor_code'] = '0000000059021'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":7, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['BA','CE','GO','MG','MS','PB','PE','PI','PR','RJ','SE']

        
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==17
        conn.close()
    except Exception as e:          
        assert False,repr(e)


def test_Sql_filterIndustriaLealdade5_plu1072870():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['1072870']
        filters[0]['modalidade']['subgrupo'] = '004'
        filters[0]['modalidade']['grupo'] = '565'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0303'
        filters[0]['modalidade']['vendor_code'] = '0000000059021'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":5, "sensibilidadePreco":"0", "un":[{"codigo":"01019604163005", "descricao":"FARINHA DE TRIGO"},{"codigo":"1019611061", "descricao":"FARINHA DE TRIGO"},{"codigo":"1019615016", "descricao":"ACUCAR REFINADO"}], "operador":"AND"} 
        filters[0]['regioes'] = ['BA','CE','GO','MG','MS','PB','PE','PI','PR','RJ','SE']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==670
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade10_plu1072870():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 10 
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['1072870']
        filters[0]['modalidade']['subgrupo'] = '004'
        filters[0]['modalidade']['grupo'] = '565'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0303'
        filters[0]['modalidade']['vendor_code'] = '0000000059021'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":10, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['BA','CE','GO','MG','MS','PB','PE','PI','PR','RJ','SE']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==1445
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade20_plu1072870():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 20
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['1072870']
        filters[0]['modalidade']['subgrupo'] = '004'
        filters[0]['modalidade']['grupo'] = '565'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0303'
        filters[0]['modalidade']['vendor_code'] = '0000000059021'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":20, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['BA','CE','GO','MG','MS','PB','PE','PI','PR','RJ','SE']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==11
        conn.close()
    except Exception as e:          
        assert False,repr(e)


def test_Sql_filterIndustriaLealdade6_plu1072613():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 6  
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['1072613']
        filters[0]['modalidade']['subgrupo'] = '001'
        filters[0]['modalidade']['grupo'] = '600'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0049'
        filters[0]['modalidade']['vendor_code'] = '0000000007394'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":6, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = None
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==54
        conn.close()
    except Exception as e:          
        assert False,repr(e)


def test_Sql_filterIndustriaLealdade7_plu1072613():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['1072613']
        filters[0]['modalidade']['subgrupo'] = '001'
        filters[0]['modalidade']['grupo'] = '600'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0049'
        filters[0]['modalidade']['vendor_code'] = '0000000007394'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":7, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = None

        
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==27
        conn.close()
    except Exception as e:          
        assert False,repr(e)


def test_Sql_filterIndustriaLealdade5_plu1072613():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['1072613']
        filters[0]['modalidade']['subgrupo'] = '001'
        filters[0]['modalidade']['grupo'] = '600'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0049'
        filters[0]['modalidade']['vendor_code'] = '0000000007394'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":5, "sensibilidadePreco":"0", "un":[{"codigo":"01019604163005", "descricao":"FARINHA DE TRIGO"},{"codigo":"1019611061", "descricao":"FARINHA DE TRIGO"},{"codigo":"1019615016", "descricao":"ACUCAR REFINADO"}], "operador":"AND"} 
        filters[0]['regioes'] = None
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==1928
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade10_plu1072613():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 10 
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['1072613']
        filters[0]['modalidade']['subgrupo'] = '001'
        filters[0]['modalidade']['grupo'] = '600'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0049'
        filters[0]['modalidade']['vendor_code'] = '0000000007394'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":10, "sensibilidadePreco":"0", "operador":"AND"}
        #filters[0]['regioes'] = ['BA','CE','GO','MG','MS','PB','PE','PI','PR','RJ','SE']
        filters[0]['regioes'] = None
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==3476
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade20_plu1072613():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 20
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['1072613']
        filters[0]['modalidade']['subgrupo'] = '001'
        filters[0]['modalidade']['grupo'] = '600'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0049'
        filters[0]['modalidade']['vendor_code'] = '0000000007394'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":20, "sensibilidadePreco":"0", "operador":"AND"}
        #filters[0]['regioes'] = ['BA','CE','GO','MG','MS','PB','PE','PI','PR','RJ','SE']
        filters[0]['regioes'] = None
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==32
        conn.close()
    except Exception as e:          
        assert False,repr(e)


def test_Sql_filterIndustriaLealdade6_familia4076810():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 6  
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['4076810']
        filters[0]['modalidade']['familia'] ='9802' 
        filters[0]['modalidade']['tipotarget'] ='FAMILIA' 
        filters[0]['modalidade']['subgrupo'] = '002'
        filters[0]['modalidade']['grupo'] = '504'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '0000000076905'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":6, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==0

        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade6_familia4076810():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 6  
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['4076810']
        filters[0]['modalidade']['familia'] ='9802' 
        filters[0]['modalidade']['tipotarget'] ='FAMILIA' 
        filters[0]['modalidade']['subgrupo'] = '002'
        filters[0]['modalidade']['grupo'] = '504'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '0000000076905'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":6, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==706
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade7_familia4076810():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 7  
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['4076810']
        filters[0]['modalidade']['familia'] ='9802' 
        filters[0]['modalidade']['tipotarget'] ='FAMILIA' 
        filters[0]['modalidade']['subgrupo'] = '002'
        filters[0]['modalidade']['grupo'] = '504'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '0000000076905'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":7, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==409
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)


def test_Sql_filterIndustriaLealdade10_familia4076810():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 10  
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['4076810']
        filters[0]['modalidade']['familia'] ='9802' 
        filters[0]['modalidade']['tipotarget'] ='FAMILIA' 
        filters[0]['modalidade']['subgrupo'] = '002'
        filters[0]['modalidade']['grupo'] = '504'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '0000000076905'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":10, "sensibilidadePreco":"0", "operador":"OR"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==296
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade5_familia4076810():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 5 
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['4076810']
        filters[0]['modalidade']['familia'] ='9802' 
        filters[0]['modalidade']['tipotarget'] ='FAMILIA' 
        filters[0]['modalidade']['subgrupo'] = '002'
        filters[0]['modalidade']['grupo'] = '504'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '0000000076905'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":5, "sensibilidadePreco":"0", "un":[{ "codigo": "01010108386", "descricao": "AGUA" }, { "codigo": "01010104592", "descricao": "VINHOS BRANCOS" }, { "codigo": "01010108503", "descricao": "CHA PRONTO" }], "operador":"OR"} 
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==11680
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade20_familia4076810():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
       
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['produto'] = ['4076810']
        filters[0]['modalidade']['familia'] ='9802' 
        filters[0]['modalidade']['tipotarget'] ='FAMILIA' 
        filters[0]['modalidade']['subgrupo'] = '002'
        filters[0]['modalidade']['grupo'] = '504'
        filters[0]['modalidade']['subcategoria'] = '08'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '0000000076905'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":20, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==433
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)


def test_Sql_filterIndustriaLealdade6_subgrupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='SUB-GRUPO'  
        filters[0]['modalidade']['subgrupo'] = '001'
        filters[0]['modalidade']['grupo'] = '528'
        filters[0]['modalidade']['subcategoria'] = '09'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '159753654'
        filters[0]['filtros'] = {"genero":["M"], "idade":[2, 3, 4], "lealdadeProduto":6, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==47
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade7_subgrupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='SUB-GRUPO'  
        filters[0]['modalidade']['subgrupo'] = '001'
        filters[0]['modalidade']['grupo'] = '528'
        filters[0]['modalidade']['subcategoria'] = '09'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '159753654'
        filters[0]['filtros'] = {"genero":["M"], "idade":[ 2, 3, 4], "lealdadeProduto":7, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==40
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade5_subgrupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 5 
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['tipotarget'] ='SUB-GRUPO'  

        filters[0]['modalidade']['subgrupo'] = '001'
        filters[0]['modalidade']['grupo'] = '528'
        filters[0]['modalidade']['subcategoria'] = '09'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '159753654'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[ 2, 3, 4,5], "lealdadeProduto":5, "sensibilidadePreco":"0","operador":"AND","un": [ { "codigo": "01010108386", "descricao": "AGUA" }]} 
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==8954
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade10_subgrupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='SUB-GRUPO'  
        filters[0]['modalidade']['subgrupo'] = '001'
        filters[0]['modalidade']['grupo'] = '528'
        filters[0]['modalidade']['subcategoria'] = '09'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '159753654'
        filters[0]['filtros'] = {"genero":["M"], "idade":[2, 3, 4], "lealdadeProduto":10, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==589
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade20_subgrupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='SUB-GRUPO'  
        filters[0]['modalidade']['subgrupo'] = '001'
        filters[0]['modalidade']['grupo'] = '528'
        filters[0]['modalidade']['subcategoria'] = '09'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '5697'
        filters[0]['modalidade']['vendor_code'] = '159753654'
        filters[0]['filtros'] = {"genero":["M"], "idade":[ 2, 3, 4], "lealdadeProduto":20, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==17
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade5_grupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 5 
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['tipotarget'] ='GRUPO'  

        filters[0]['modalidade']['grupo'] = '015'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '114'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0000'
        filters[0]['modalidade']['vendor_code'] = '0000000002433'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":5, "sensibilidadePreco":"0", "un":[{"codigo":"01010101565", "descricao":"FARINHA DE TRIGO"},{"codigo":"01010101598", "descricao":"ACUCAR REFINADO"},{"codigo":"01010101535", "descricao":"ACUCAR REFINADO"}], "operador":"AND"} 
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==1876
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)


def test_Sql_filterIndustriaLealdade6_grupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='GRUPO'  

        filters[0]['modalidade']['grupo'] = '015'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '114'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0000'
        filters[0]['modalidade']['vendor_code'] = '0000000002433'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[ 2, 3, 4, 5], "lealdadeProduto":6, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==1022
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade7_grupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='GRUPO'  

        filters[0]['modalidade']['grupo'] = '015'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '114'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0000'
        filters[0]['modalidade']['vendor_code'] = '0000000002433'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[ 2, 3, 4, 5], "lealdadeProduto":7, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==1170
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade10_grupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='GRUPO'  

        filters[0]['modalidade']['grupo'] = '015'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '114'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0000'
        filters[0]['modalidade']['vendor_code'] = '0000000002433'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[2, 3, 4, 5], "lealdadeProduto":10, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==1608
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade20_grupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='GRUPO' 

        filters[0]['modalidade']['grupo'] = '015'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '114'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0000'
        filters[0]['modalidade']['vendor_code'] = '0000000002433'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[ 2, 3, 4, 5], "lealdadeProduto":20, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==708
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade5_grupo():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        filters[0]['filtros']['lealdadeProduto'] = 5 
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        filters[0]['modalidade']['tipotarget'] ='GRUPO'  

        filters[0]['modalidade']['grupo'] = '015'
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '114'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '0000'
        filters[0]['modalidade']['vendor_code'] = '0000000002433'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[ 2, 3, 4, 5], "lealdadeProduto":5, "sensibilidadePreco":"0", "un": [ { "codigo": "01010101565" }, { "codigo": "01010101598" }, { "codigo": "01010101535" }], "operador":"OR"} 
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==10729
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)


def test_Sql_filterIndustriaLealdade6_subcategoria():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='SUB-CATEGORIA'    
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '7893'
        filters[0]['modalidade']['vendor_code'] = '123456789'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[ 2, 3, 4, 5], "lealdadeProduto":6, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==61
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade7_subcategoria():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='SUB-CATEGORIA'    
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '7893'
        filters[0]['modalidade']['vendor_code'] = '123456789'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[ 2, 3, 4, 5], "lealdadeProduto":7, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==385
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade10_subcategoria():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='SUB-CATEGORIA'    
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '7893'
        filters[0]['modalidade']['vendor_code'] = '123456789'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[ 2, 3, 4, 5], "lealdadeProduto":10, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==3626
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade20_subcategoria():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='SUB-CATEGORIA'    
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '7893'
        filters[0]['modalidade']['vendor_code'] = '123456789'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[ 2, 3, 4, 5], "lealdadeProduto":20, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==277
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade5_subcategoria():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='SUB-CATEGORIA'    
        filters[0]['modalidade']['subcategoria'] = '01'
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '7893'
        filters[0]['modalidade']['vendor_code'] = '123456789'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[ 2, 3, 4, 5], "lealdadeProduto":5, "sensibilidadePreco":"0", "un": [ { "codigo": "01010101565" }, { "codigo": "01010101598" }, { "codigo": "01010101535" }], "operador":"OR"} 
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==11344
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade6_categoria():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='CATEGORIA'    
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '7893'
        filters[0]['modalidade']['vendor_code'] = '123456789'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":6, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==51
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade7_categoria():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='CATEGORIA'    

        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '7893'
        filters[0]['modalidade']['vendor_code'] = '123456789'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":7, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==730
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade10_categoria():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='CATEGORIA'    
     
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '7893'
        filters[0]['modalidade']['vendor_code'] = '123456789'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":10, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()

        
    
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==6956
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade20_categoria():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='CATEGORIA'    
     
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '7893'
        filters[0]['modalidade']['vendor_code'] = '123456789'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":20, "sensibilidadePreco":"0", "operador":"AND"}
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==367
        
        conn.close()
    except Exception as e:          
        assert False,repr(e)

def test_Sql_filterIndustriaLealdade5_categoria():
    try:
        filters = data_Sql_Modalidade.get('audience')
        
        
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        filters[0]['modalidade']['tipotarget'] ='CATEGORIA'    
 
        filters[0]['modalidade']['categoria'] = '101'
        filters[0]['modalidade']['departamento'] = '010'
        filters[0]['modalidade']['vendor_class_code'] = '7893'
        filters[0]['modalidade']['vendor_code'] = '123456789'
        filters[0]['filtros'] = {"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":5, "sensibilidadePreco":"0", "un": [ { "codigo": "01010101565" }, { "codigo": "01010101598" }, { "codigo": "01010101535" }], "operador":"AND"} 
        filters[0]['regioes'] = ['SP']
        obj = GetFilter('INDUSTRIA').get(filters[0])               
        ret,conn = getSql_filters(obj)

        ret = ret.replace('\n','').strip()
        ret = ' '.join(ret.split()).lower()
        
        
        result,conn= execute_query_temp(ret,conn)
        assert  result[0][0]==236
        
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
