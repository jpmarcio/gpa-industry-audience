import os
import pandas as pd

if os.name !='nt':
    import ConfigParser as configparser
else:
    import configparser as configparser

import psycopg2
import psycopg2.extras
from queryreports import QueryReports

config = configparser.ConfigParser()
config_files = ['config.ini','config_connections.ini']
config.read(config_files) 
# conn_string = config.get('Connections', 'gcloud2')
conn_string = config.get('Connections', 'aws3')

class Report(object):


    def getIndicadores(self):
        """
            Returns data for the home of MD
            Parameters None
        """

        plataforma  = getinformacoesPlataforma()
        industria  = getinformacoesIndustria()

        retorno = {}

        retorno['Plataforma'] = plataforma.to_dict('records')
        retorno['Industria'] = industria.to_dict('records')
    
        return retorno

    def getReportData(self, filters):
        """
            get report data based on parameters

            Parameters:            
            "vendor_class_code": "string", Codigo do fabricante ex:5498 'Schincariol', para GPA Deixar em branco
            "vendor_code": "string", Codigo do Distribuidor ex :00000000028067 'HNK BR BEBIDAS LTDA.'. para GPA deixar em branco
            "regioes": [
                "string"

            ],
                    Matriz com as regioes a serem consideradas.
                    Enviar lista com as siglas dos estados "bandeira": "string",
                    Tipo de fidelidade: Valores admitidos :PA, EX. O comportamento padrao caso nao seja enviado eh calculo para as duas bandeiras juntas
        
        """
        
        vendorCode,vendorclassCode,regioes,bandeira = getfiltersariables(filters)

        infgerais  = getinformacoesgerais(vendorCode,vendorclassCode,regioes,bandeira)
        dfclientes  = getClientes(vendorCode,vendorclassCode,regioes,bandeira)
        dfvendasindustria  = getvendasindustria(vendorCode,vendorclassCode,regioes,bandeira)
        dfultimosciclos = getultimosciclos(vendorCode,vendorclassCode,regioes,bandeira)
        dftopciclos = gettopciclos(vendorCode,vendorclassCode,regioes,bandeira)


        report = []
        for index,row in infgerais.iterrows():
            fornecedor = {}
            fornecedor['nome'] = row['vendor_name']+ " - "+ row['vendor_class_name']
            cli = dfclientes[(dfclientes['vendor_name']+ " - "+ dfclientes['vendor_class_name'])==fornecedor['nome']]
            vendasindustria = dfvendasindustria[(dfvendasindustria['vendor_name']+ " - "+ dfvendasindustria['vendor_class_name'])==fornecedor['nome']]
            ultimosciclos = dfultimosciclos[(dfultimosciclos['vendor_name']+ " - "+ dfultimosciclos['vendor_class_name'])==fornecedor['nome']]
            topciclosdf = dftopciclos[(dftopciclos['vendor_name']+ " - "+ dftopciclos['vendor_class_name'])==fornecedor['nome']]

            vi = []
            sharevendas = {}
            sharevendas['vendasmeudesconto'] = sum(vendasindustria['md_sales'])
            sharevendas['totalvendas'] = sum(vendasindustria['total_store_sales'])
            sharevendas['sharevendas'] = sharevendas['vendasmeudesconto'] /sharevendas['totalvendas']*100

            for index,venda in  vendasindustria.iterrows():             
                v = {}
                v['mesano'] =venda['period']
                v['totalvendas'] =venda['md_sales'] 
                vi.append(v)

            ultciclos =[]
            for index,ciclo in  ultimosciclos.iterrows():             
                u = {}
                u['mesano'] =ciclo['wave']
                u['totalvendas'] =float(ciclo['md_sales']) 
                ultciclos.append(u)

            topciclos =[]
            for index,ciclo in  topciclosdf.iterrows():             
                u = {}
                u['mesano'] =ciclo['wave']
                u['totalvendas'] =float(ciclo['md_sales']) 
                topciclos.append(u)


            informacoesgerais ={}            
            informacoesgerais['totalportifolio'] = int(row['num_prods_total'])
            informacoesgerais['participacao'] = float(row['percentualMeudesconto'])
            informacoesgerais['produtosmeudesconto'] = int(row['num_prods'])

            ciclos ={}            
            ciclos['ultimaonda'] = row['last_wave_total']
            ciclos['ultimaparticipacao'] = row['last_wave_participated']
            ciclos['primeiraparticipacao'] = row['first_wave_participated']
            ciclos['numeroparticipacoes'] = int(row['num_waves_participated'])
          

            funil ={}
            funil['clientesalocados'] = int(row['num_clients_allocated'])
            funil['ofertasativadas'] = int(row['activated_offers'])
            funil['ofertascompradas'] = int(row['bought_offers'])

          
          
            clir = {}
            try:
                clir['clientescategoria'] =int(cli["total_store_industry_clients"])
                clir['seusclientes'] =int(cli["total_store_category_clients"])
                clir['clientescategoriameudesconto'] =int(cli["md_industry_clients"])
                clir['seusclientescategoriameudesconto'] =int(cli["md_category_clients"])
            except Exception as e:
                print()    
            


            dados = {}
            dados['informacoesgerais'] = informacoesgerais
            dados['funil'] = funil
            dados['clientes'] = clir
            dados['vendasindustria'] = vi
            dados['sharevendas'] = sharevendas
            dados['ciclos'] = ciclos
            dados['ultimosciclos'] = ultciclos
            dados['topciclos'] = topciclos

            fornecedor['dados'] = dados
            report.append(fornecedor)

        
        return {"fornecedor":report}

    
def getinformacoesgerais(vendorCode,vendorclassCode,regioes,bandeira):
    """

        return data to informacoes gerais section 
    """

    

    strQuery = QueryReports().informacoesgerais(vendorCode,vendorclassCode,regioes,bandeira)

    result = execute_query(strQuery)
    result = pd.DataFrame(result, columns=['num_prods_total','num_prods','percentualMeudesconto','num_clients_allocated','bought_offers','activated_offers',
                'first_wave_participated', 'last_wave_participated',
                'num_waves_participated', 'last_wave_total',
                'vendor_class_code','vendor_code','vendor_class_name','vendor_name'])
    return result

def getClientes(vendorCode,vendorclassCode,regioes,bandeira):
    """

        return data to clientes section 
    """

    

    strQuery = QueryReports().clientes(vendorCode,vendorclassCode,regioes,bandeira)
 
    result = execute_query(strQuery)
    result = pd.DataFrame(result, columns=['vendor_class_code','vendor_code','vendor_class_name','vendor_name'
                ,'total_store_category_clients',  'total_store_industry_clients' 
                ,  'md_category_clients','md_industry_clients','total_store_industry_penetration' 
                ,  'md_industry_penetration'])
    return result


def getvendasindustria(vendorCode,vendorclassCode,regioes,bandeira):
    """
        return data to vendas industria section 
    """    

    strQuery = QueryReports().vendasindustria(vendorCode,vendorclassCode,regioes,bandeira)
 
    result = execute_query(strQuery)
    result = pd.DataFrame(result, columns=['vendor_class_code','vendor_code','vendor_class_name','vendor_name',
               'period','total_store_sales','md_sales'])
    return result

def getultimosciclos(vendorCode,vendorclassCode,regioes,bandeira):
    """

        return data to vendas industria section 
    """

    strQuery = QueryReports().ultimosciclos(vendorCode,vendorclassCode,regioes,bandeira)
 
    result = execute_query(strQuery)
    result = pd.DataFrame(result, columns=['vendor_class_code','vendor_code','vendor_class_name','vendor_name',
               'wave','md_sales'])
    return result


def gettopciclos(vendorCode,vendorclassCode,regioes,bandeira):
    """

        return data to vendas industria section 
    """

    strQuery = QueryReports().topciclos(vendorCode,vendorclassCode,regioes,bandeira)
 
    result = execute_query(strQuery)
    result = pd.DataFrame(result, columns=['vendor_class_code','vendor_code','vendor_class_name','vendor_name',
               'wave','md_sales'])
    return result

def getinformacoesIndustria():
    """

        return data to indicadores industria section 
    """

    strQuery = QueryReports().IndicadoresIndustria()
 
    result = execute_query(strQuery)
    result = pd.DataFrame(result, columns=['vendor_class_code','vendor_code','bandeira', 'ofertaAtivadas','clientesComprando','clientesUnicos','vendas'])
    return result


def getinformacoesPlataforma():
    """

        return data to indicadores industria section 
    """

    strQuery = QueryReports().IndicadoresPlataforma()
 
    result = execute_query(strQuery)
    result = pd.DataFrame(result, columns=['bandeira', 'ofertaAtivadas','clientesComprando','vendas'])
    return result      

def getfiltersariables(filters):
    """
        return parametes to filter the data from json 
    """
    bandeira = ""
    regioes = []
    vendorCode  = ""
    vendorclassCode = ""

    dic = {"1":'00040000', "2":'00040100', "3":'00040200',"4":'00040300'}

    
        
    if 'bandeira'  in filters.keys():
        bandeira = filters.get('bandeira')    
        if bandeira== 'EX':
            bandeira = 'CLEX'
        if bandeira is None:
            bandeira = ''

    if 'regioes' in filters.keys():
        r = filters.get('regioes')  
        regioes = [dic.get(n, n) for n in r]

    if 'vendor_class_code' in filters.keys():
        vendorclassCode = filters.get('vendor_class_code')  
    
    if 'vendor_code' in filters.keys():
        vendorCode = filters.get('vendor_code')  

    return vendorCode,vendorclassCode,regioes,bandeira
    

                      
def execute_query(query):
    print("\n" + query)
    # todo Trocar para pegar do config
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    cursor.close()
    return result
