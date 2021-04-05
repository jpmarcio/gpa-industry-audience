from utilsobjects import classproperty, state_func, listdecode2str, numlist2str,list2str, gender_func, sens_func
from queries import Queries

from errorobjects import L20notfound
from aux_data import *
from createfilter import GetFilter
import traceback
import psycopg2
import psycopg2.extras
import pandas as pd
import os
from report import Report

if os.name !='nt':
    import ConfigParser as configparser
else:
    import configparser as configparser



config = configparser.ConfigParser()
config_files = ['config.ini','config_connections.ini']
config.read(config_files) 
# conn_string = config.get('Connections', 'gcloud2')
conn_string = config.get('Connections', 'aws3')

class MeuDesconto(object):

    def get_dataToFilters(self):
        """
            Get data that  will be used on application to construct filters
        """ 
        
        return get_filter_data()
    
    def getReportData(self,filters):
        """
            Get data  report data
        """ 
        rep = Report()

        return rep.getReportData(filters)

    def getIndicadores(self):
        """
            Get data  for indicadores MD
        """ 
        rep = Report()

        return rep.getIndicadores()

        

        
    def calcular_audiencia_industria(self,audience_filters):  
        """
            Calculate Audience based on parameters passed
            Parameters:
                Audience_filters: filters to calculate audience
                
        """
        audience_cnt = 0
       
        units_to_calc = 0 
        avg_price = 0.0
        level_conversion = 0.0
        vendor = ""

        duracao = audience_filters.get('duracao')
        if not duracao:
            duracao = 3
        conn = None
        try:

            fidelidade = audience_filters.get("pFidelidade")
            
            audience_cnt,conn, avg_price_clex,avg_price_pa,index_info,level_conversion,vendor = get_Data_from_db(audience_filters, audience_filters.get('modalidade'),'INDUSTRIA')
            if not audience_cnt:
                audience_cnt = 0
            
            avg_price = 0 
            if  fidelidade =="EX":
                avg_price =avg_price_clex
            elif fidelidade =="PA" or fidelidade =="PA+":
                avg_price =avg_price_pa
            else:
                avg_price = (avg_price_clex + avg_price_pa)/2


            units_per_visit = round(float(index_info['units_per_visit']))
            units_to_calc = units_per_visit
            discounts = calc_discounts(float(index_info['min_perc_disc']),float(avg_price))

            # Limits the quantity to be used for calculations
            if audience_filters.get("unidadesMaxProduto") and float(units_per_visit) >= float(str(audience_filters.get("unidadesMaxProduto"))):
                units_to_calc = audience_filters.get("unidadesMaxProduto")

            est_publico_alocado, est_investimento_maximo, est_faturamento = calculo_estimativo(audience_cnt,
                                                                                        audience_filters,
                                                                                        round(float(units_to_calc)),
                                                                                        duracao,
                                                                                        float(avg_price),
                                                                                        level_conversion,
                                                                                        vendor,
                                                                                        conn)
        except Exception as e:
            print (e)
            traceback.print_exc()
            raise e

        
                                                                                        
        try:
            success_response = {"estFaturamento": est_faturamento,
                                "estPublicoAlocado": est_publico_alocado,
                                "estInvestimentoMaximo": est_investimento_maximo,
                                "valorAudiencia": int(audience_cnt),
                                "qtdMediaCliente": units_per_visit,
                                "MinDiscountPercentual": index_info['min_perc_disc'],
                                "MinDiscountAbs": round(discounts['mindiscount'], 2),
                                "MaxDiscountAbs": round(discounts['maxdiscount'], 2),
                                "pesoVariavel": bool(index_info['weight_flag']),
                                "MaxNumberUN": 3,
                                "MinNumberUN": 1,
                                "allowUnder18": not bool(index_info['is_alcohol']),
                                "pFidelidade": audience_filters.get('pFidelidade'),
                                "preco": float(avg_price)}
            if audience_filters.get('id'):
                success_response['id'] = audience_filters.get('id')
            conn.close()
            return success_response
        except Exception as e:
            print (e)
            traceback.print_exc()
            if not conn is None:
                conn.close()
            return None


def get_Data_from_db(audience_filters, modalidade, plataforma):
    """
        Given the parameters calculates the audience  and the average price. Get max units per visit and min_perc_disc
        Parameters:
            audience_filters: audience filters to take in consideration
            index_info: information about de product        
    """

    units_per_visit =0
    min_perc_disc =0 
    
    
    #f
    if audience_filters['modalidade']['tipotarget'] in ('PLU','FAMILIA'):
        produtos = audience_filters['modalidade']['produto']
        produto = ""
        if len(produtos)> 0:
            produto = produtos[0]
        else:
            raise Exception("Produto nao preenchido")

         
        queryprod = Queries().getProductInfo()
        queryprod = queryprod.format(produto)  
        result = execute_query(queryprod)
        result = pd.DataFrame(result, columns=['prod_hier_l10_code'])

        if len(result)==0:
            raise Exception("Nao foram encontrados produtos(cubos) para esta selecao")

        prodinfo = str(result['prod_hier_l10_code'][0])
        audience_filters['modalidade']['departamento']= prodinfo[:3]        
        audience_filters['modalidade']['categoria']= prodinfo[3:6]
        audience_filters['modalidade']['subcategoria']= prodinfo[6:8]        
        audience_filters['modalidade']['grupo']= prodinfo[8:11]
        audience_filters['modalidade']['subgrupo']= prodinfo[11:14]
        
    obj = GetFilter(plataforma).get(audience_filters)   
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
    result,conn = execute_query_temp(ret.replace('\n','').strip() ,conn)

    avgpricequery = obj.get_average_price()

    resultavg,conn = execute_query_temp(avgpricequery,conn)
    resultavg = pd.DataFrame(resultavg, columns=['avg_prod_price_clex','avg_prod_price_pa'])
    avg_price_clex,avg_price_pa = resultavg['avg_prod_price_clex'][0],resultavg['avg_prod_price_pa'][0]

    index_info = get_index_info(conn,audience_filters.get('modalidade'))


    return result[0][0],conn,avg_price_clex,avg_price_pa, index_info,level_conversion,obj._vendor
    

def get_filter_data():
        
        
        l22_distinct = execute_query(
            'select distinct prod_comml_l22_desc, prod_comml_l22_code from obj_structure_count')

        
        category = []

        for l22 in l22_distinct:

            result = execute_query('''select distinct prod_comml_lxx_code, level, prod_comml_lxx_desc
                                    from obj_structure_count where prod_comml_l22_desc
                                    in ('{}')'''.format(l22.prod_comml_l22_desc))

            un_group = []
            for un in result:
                un = {
                    'un_code': un.prod_comml_lxx_code,
                    'un_level': un.level,
                    'un_name': un.prod_comml_lxx_desc
                }
                un_group.append(un)

            cat = {
                'UN': un_group,
                'cat_id': l22.prod_comml_l22_code,
                'name': l22.prod_comml_l22_desc
            }
            category.append(cat)

        return {"categories": category}


def get_index_info(conn,modalidade):
    """
        Get basic information about product or L20
    """
    query = Queries().IndexInfoProd().replace('\n','').strip()
    prod_code = None
   
    try:
        # try to find data for l20 if product wasnt found
        result,conn = execute_query_temp(query,conn)
        result =result[0]
        if result[0] is None:
            query = Queries().IndexInfoL20().replace('\n','').strip()
            result,conn = execute_query_temp(query,conn)
            result =result[0]
            if result[0] is None:
                raise Exception()

    except Exception as e:
        print("\n**** Too Bad! It seems  your l20 wasnt found either! ****")           
        raise L20notfound("Nao encontrado dados para os seguintes parametros: {}".format(str(modalidade)))
    
    # return information about l20 or product
    return {
    
            'units_per_visit': result[0],
            'min_perc_disc': result[1],
            'weight_flag': result[2],
            'is_alcohol': result[3]
           
            }


def calc_discounts(percentualmin, preco):
    """ 
     Calculates the minimum and maximum amount of discount expected for each unit
     Parameters:
      - percentualmin: Percentage which represents the minimum discount from settings
      - preco: Average price for the product in context
    """
    if not (percentualmin and preco):
        return {'maxdiscount': 0.0, 'mindiscount': 0.0}

    maxdiscount = preco * 0.5
    percentual_minimo = float(percentualmin) / 100
    mindiscount = preco * percentual_minimo

    return {'maxdiscount': maxdiscount, 'mindiscount': mindiscount}


def calculo_estimativo(audience_count, audience, qtdmedia_unidades,duracao,price,level,vendor_code, conn):
    """
        Calculates estimations of expenditures and return according to averages and coeficients
    """
    banner = audience.get("pFidelidade")
    reward = 0.0073
    if banner == "PA":
        reward = 0.0116
    aquisition = 0.0010

    coeficientes = {'frequency': reward, 'loyal_a': reward, 'loyal_b': reward, 'ticket': reward,
                    'penetration': aquisition, 'recover': aquisition, 'launch': aquisition}

    coeficiente = coeficientes.get(audience.get("objetivo"), reward)
    redemption_cat_type = 2
    # if payload['lealdadeProduto'] == 1:  # acima de 50%
    # elif payload['lealdadeProduto'] == 2:  # abaixo de 50#
    # elif payload['lealdadeProduto'] == 3:  # compra no L20 e nao o produto
    # elif payload['lealdadeProduto'] == 4:  # nao compra no l20
    # elif payload['lealdadeProduto'] == 5:  # nao compra o produto
    # if payload['lealdadeProduto'] == 6:  # SPENDS MOST WITH MY PRODUCT%
    # elif payload['lealdadeProduto'] == 7:  # DOES NOT SPEND MOST WITH MY PRODUCT#
    # if payload['lealdadeProduto'] == 10:  # PRODUCT LAUNCH NEW
    # elif payload['lealdadeProduto'] == 20:  # RECOVER PRODUCT NEW #

    if audience.get('filtros'):
        filtros = audience['filtros'].get('lealdadeProduto')
        if filtros in (1, 2, 6, 7, 20):
            coeficiente = reward
            redemption_cat_type = 1
        else:
            coeficiente = aquisition
            redemption_cat_type = 2

    level_code = level
    c = get_redemption(banner, level_code, redemption_cat_type,vendor_code,conn)
    print('get_redemption_by_lxx({},{},{}) => c = {}'.format(banner, level_code, redemption_cat_type, c))
    if c:
        coeficiente = c
    else:
        if level == 'L20':
            level_code = 'L30'
        else:
            level_code = 'L40'
    
        c = get_redemption(banner, level_code, redemption_cat_type,vendor_code,conn)
        print('get_redemption_by_lxx({},{},{}) => c = {}'.format(banner, level_code, redemption_cat_type, c))
        if c:
            coeficiente = c

    coeficiente_duracao = 1.0 #14 days
    if duracao == 1:
        coeficiente_duracao = 0.6 # 1st week
    elif duracao == 2:
        coeficiente_duracao = 0.4 # second week
    

    est_publico_alocado = round(float(audience_count) * float(coeficiente)*coeficiente_duracao)
    print("est_publico_alocado = audience_count[{}] * coeficiente[{}] * coeficiente_duracao[{}]".format(audience_count,coeficiente,coeficiente_duracao))

    has_mecanica = ('mecanica' in audience and
                    audience['mecanica'] and
                    'tipoMecanica' in audience['mecanica'])

    if has_mecanica:
        mecanica = audience['mecanica']
        tipo_mecanica = str(mecanica['tipoMecanica'])
        preco_produto = price

        if tipo_mecanica == "LEVE_X_PAGUE_Y":
            unidades_pagas = float(str(mecanica.get("unidadesPagas", 0)).replace(",", "."))
            unidades_levadas = float(str(mecanica.get("unidadesLevadas", 0)).replace(",", "."))
            discount_per_unit = float((1.0 - unidades_pagas / unidades_levadas) * preco_produto)

            est_investimento_maximo = discount_per_unit * est_publico_alocado * (
                qtdmedia_unidades // unidades_levadas)
            est_faturamento = (preco_produto - discount_per_unit) * est_publico_alocado * (
                qtdmedia_unidades // unidades_levadas)

        elif tipo_mecanica == "PERCENTUAL_X_NA_Y_UNIDADE":
            desconto_em_outra_unidade = float(str(mecanica.get("descontoEmOutraUnidade", 0)).replace(",", "."))
            unidades = float(str(mecanica.get("unidades", 0)).replace(",", "."))
            discount_per_unit = float(desconto_em_outra_unidade / unidades * preco_produto)

            est_investimento_maximo = discount_per_unit * est_publico_alocado * (qtdmedia_unidades // unidades)
            est_faturamento = (preco_produto - discount_per_unit) * est_publico_alocado * (
                qtdmedia_unidades // unidades)

        elif (tipo_mecanica == "PERCENTUAL_X") or (tipo_mecanica == "PERCENTUAL"):
            valor_desconto = float(str(mecanica.get("valorDesconto", 0)).replace(",", "."))
            discount_per_unit = float(valor_desconto * preco_produto)

            est_investimento_maximo = discount_per_unit * est_publico_alocado * qtdmedia_unidades
            est_faturamento = (preco_produto - discount_per_unit) * est_publico_alocado * qtdmedia_unidades

        else:  # ABSOLUTO_X or ABSOLUTO
            discount_per_unit = float(str(mecanica.get("valorDesconto", 0)).replace(",", "."))

            est_investimento_maximo = discount_per_unit * est_publico_alocado * qtdmedia_unidades
            est_faturamento = (preco_produto - discount_per_unit) * est_publico_alocado * qtdmedia_unidades
    else:
        est_investimento_maximo = 0
        est_faturamento = 0

    return round(est_publico_alocado), round(est_investimento_maximo), round(est_faturamento)

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

def execute_query_temp(query,conn):
    print("\n" + query)
    # todo Trocar para pegar do config
    if conn is None:
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    cursor.execute(query)
    result = cursor.fetchall()
    #conn.close()
    cursor.close()
    return result, conn

def get_redemption(banner, level_code, audience_type,vendor, conn):

    query = Queries().conversion_rates().format(level=level_code) 
    if banner:
        query += " AND  banner ='{}' ".format(banner)
    
    if audience_type:
        query += " AND  aba = '{}'".format(audience_type)

    if vendor!="":
        query += " AND  cr.supplier_code = '{}'".format(vendor)

    result,conn = execute_query_temp(query,conn)
    result = pd.DataFrame(result)

    
    if result['conversion_rate'][0] == 0:
        message = "Redemption rate not found for level {l}, aba {a}, banner {b}".format(l=level_code,a=audience_type,b=banner)
        print(message)
        try:
            now = datetime.now()
            f = open("L20ConversionNotfound.log", "a")
            f.write("\n{0} - Erro: {1} ".format(now.strftime("%d/%m/%Y %H:%M:%S"), message))
            f.flush()
            f.close()
        finally:
            r = False        
    else:        
        r = result['conversion_rate'][0]
        r = format(r, '.4f')
    return r
