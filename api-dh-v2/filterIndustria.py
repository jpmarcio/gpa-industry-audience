from utilsobjects import classproperty, state_func, listdecode2str, numlist2str,list2str, gender_func, sens_func
from queries import Queries
from aux_data import get_column_from_dna_id
from modalidade import Modalidade
import pandas as pd 
import traceback
from filterinterface import FilterInterface

class FilterIndustria(FilterInterface):
    def __init__(self):
        #super().__init__()
        self._TipoPlataforma = "INDUSTRIA"
        


    _filter = ''
    _TipoPlataforma = ''
    _vendor = ""
    _prod_codes = []
    _modalidade = {}
    _l30 = []


    @classproperty
    def filter(self):
        return self._filter

    @filter.setter
    def filter(self, value):
        self._filter = value
    
    @classproperty
    def TipoPlataforma(self):
        return self._TipoPlataforma 


    @TipoPlataforma.setter
    def TipoPlataforma(self, value):
        self._TipoPlataforma = value

    @classproperty
    def l30(self):
        return self._TipoPlataforma 


    @l30.setter
    def l30(self, value):
        self._l30 = value



        

    @classproperty
    def prod_codes(self):
        return self._prod_codes


    @prod_codes.setter
    def prod_codes(self, value):
        self._prod_codes = value

    @classproperty
    def modalidade(self):
        return self._modalidade


    @modalidade.setter
    def modalidade(self, value):
        self._modalidade = value
    
    def getSqlProductTemporaryTable(self):
        '''
            get sql that wil be used to create the product temporary table and return the l20 and l30 used in some queries
        '''
        self.ValidateParameters()
        
        vendor_class = str(self._modalidade._value.get('vendor_class_code')).zfill(4)
        vendor_code = str(self._modalidade._value.get('vendor_code')).zfill(13)

        vendor = ""
        if len(vendor_class)>0 and len(vendor_code)>0:
            if  vendor_class!= '0000':
                vendor  =  vendor_class    
            else: 
                vendor  =  vendor_code
        elif len(vendor_class)==0 and len(vendor_code)==0:
            vendor  = "" #GPA
        else:
            raise Exception('Fornecedor preenchido incorretamente')
        self._vendor = vendor

        return self._modalidade.getl20_30(vendor)




    def ValidateParameters(self):
        '''
            Validate parameters if they are correct according to chosen platform 
            We'll try to serialize to the corresponding platform          
        '''
        modalidade  = self._filter.get('modalidade')
        try:
            self._modalidade = Modalidade(modalidade)
        except Exception as ex:
            print(str(ex))
            raise Exception('Parametros invlidos para modalidade: {}'.format( str(ex)))
         
        if self._TipoPlataforma == 'INDUSTRIA':
            return True
            
        if self._TipoPlataforma == 'PESQUISA':
            return True

        if self._TipoPlataforma == 'ALOCACAO':
            return True


        return False
    

    

    def get_sql_filters(self):
        """
            return the sql command to calculate audience
        """
        self.ValidateParameters()
        
        nonundict = build_str_non_un_filters(self._filter)
        str_filters = build_str_gender_age_prc_sens_queries(self._filter)
   

        target_code = self._modalidade.targetcode

        target = self._modalidade._value.get("tipotarget")
  
        filters = self._filter['filtros']
        # un_code = index_info['un_code'] Commented because loyalties 3, 4 are not used
        un_code = None
        
        has_dna = ('dna' in filters and filters['dna'] and len(filters['dna']) > 0)
        str_join, str_dna, str_un  = '', '', ''

        #check if has UN in the request
        if ('un' in filters and len(filters['un']) > 0):
            for un in filters.get('un'):
                if type(un) is dict:
                    un_code = un['codigo']
                else:
                    un_code = un

                if len(str(un_code)) <= 11:
                    str_un += "{operador}  un_{field} = 1 ".format(field =str(un_code).zfill(11),operador=filters.get('operador','AND'))
                else:
                    str_un += "{operador}  un_{field} = 1 ".format(field =str(un_code).zfill(14),operador=filters.get('operador','AND'))
                
                
        # check if has DNa in the request
        if has_dna:
            print("YES --- Has DNA!")       
            str_join = Queries().innerPersonas() 
            str_dna = build_str_dna(filters['dna'], filters.get('operadorDna','AND'))

        
        #Map loyalty with the correct function
        switcher = {
            1: getQueryLoyaltyOver50,
            2: getQueryLoyaltyUnder50,
            3: getQueryLoyaltyBuysL20NotProd,
            4: getQueryDoesntBuyL20,
            5: getQueryDoesntBuyProduct,#used
            6: getQuerySpendMost,# used
            7: getQueryDoesntSpendMost,#used        
            10: getQueryLaunchProduct, #used
            20: getQueryRecover # used

        }

        if filters['lealdadeProduto']:
            #Get the function from switcher dictionary
            func = switcher.get(filters['lealdadeProduto'], lambda: "Lealdade invlida")
            #Execute the function

        else:
            filters.update({'filtros':{'idade': '', 'un': [], 'genero': '', 'operador': '', 'dna': [], 'sensibilidadePreco': '', 'operadordna': '', 'lealdadeProduto': 6}})
            print("nao tem lealdade")
            #filters['lealdadeProduto'] = 6
            func = switcher.get(filters['lealdadeProduto'], lambda: "Lealdade invlida")

       
        query = func(self._l30,target_code,target,str_un,un_code,str_filters,str_dna,nonundict['fidelidade'],nonundict['regioes'],str_join,self._filter) 
              

        return query,self._modalidade.level_conversion

    def get_average_price(self):
        """
            return average price for the products, need the connection that holds product temporary table
        """
        query = Queries().AveragePrice()
        return query
    
    

# Builds a string to be used in the WHERE clause
# concerning personas
def build_str_dna(personas_filter, operador):
    str_personas = ''
    
    try:
        # get a list of ids only...
        dna_ids = [dna['codigo'] for dna in personas_filter]
        # get the list of dna corresponding columns for each id
        list_dna_cols = get_column_from_dna_id(dna_ids)

        for dna_col in list_dna_cols:
            str_personas += ' {op} seg_value_{dna} = 1'.format(dna=dna_col, op=operador)
    except Exception:
        print("\n**** Exception : ID PERSONA UNRECOGNISED : {} ****".format(str(personas_filter)))
        traceback.print_exc()
        raise ValueError

    return 'AND (' + str_personas[(len(operador)+2):] + ')'

def getObjetivoSql(objetivo):
    objetivo_dict = {
        'frequency': ' AND is_frequency_increase_candidate = True ',
        'loyal_a': ' AND is_loyal_a = True ',
        'loyal_b': ' AND is_loyal_b = True ',
        'ticket': ' AND is_ticket_increase_candidate = True ',
        'penetration': ' AND is_product_increase_candidate_a = True ',
        'recover': ' AND is_recovery_candidate = True ',
        'launch': ' AND is_new_product_candidate = True '
    }
    if objetivo != None:
        return  objetivo_dict[objetivo]
    else:   
        return ''

def getLoyaltyProgramSql(program):
    lprogram_dict = {
            'EX': " AND loyalty_program_code in ('2') ",
            'CLEX': " AND loyalty_program_code in ('2') ",
            'PA': " AND loyalty_program_code in ('1')"
        }
    if program != None:
        return  lprogram_dict[program]
    else:    
        return "AND loyalty_program_code in ('3')"

def select_regions(state_region):
    # We filter by region only if it's available and it's not 27, that is, all regions with no exclusion
    if not state_region:
        return ''
    elif len(state_region) >= 27:
        return ''

    return " AND prsn_address_state_prov_code in {regions}".format(regions=listdecode2str(state_func, state_region))


def build_str_non_un_filters(filter):

    str_objective = ''
    if 'objetivo' in filter:
        str_objective = getObjetivoSql(filter["objetivo"])

    str_lprogram = ''
    
    if 'pFidelidade' in filter:
        str_lprogram = getLoyaltyProgramSql(filter.get('pFidelidade', 'EX'))
    else:
        if filter.get('filtros')['lealdadeProduto']==10:
            str_lprogram = getLoyaltyProgramSql(None)

    str_region = ''
    if 'regioes' in filter:
        str_region = select_regions(filter["regioes"])


    nonUnDict = {
        "objetivo" : str_objective,
        "regioes" : str_region,
        "fidelidade" : str_lprogram
    }
    return nonUnDict


def getQueryLoyaltyOver50(l21,l20,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters):
    query, table = Queries().Lealdade50()

    if len(str_dna) >0:
        str_join = str_join.format(table=table.format(l21=l21), filter_personas=str_dna)

    query = query.format(l21=l21, filters=str_filters, loyal='true',
                        program=loyalty_program, regions=regions, joins=str_join)

    return query
    
def getQueryLoyaltyUnder50(l21,l20,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters):
    query, table = Queries().Lealdade50()

    if len(str_dna) >0:
        str_join = str_join.format(table=table.format(l21=l21), filter_personas=str_dna)

    query = query.format(l21=l21, filters=str_filters, loyal='false',
                        program=loyalty_program, regions=regions, joins=str_join)

    return query

def getQueryLoyaltyBuysL20NotProd(l21,l20,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters):
    query, table_a, table_b = Queries().LealdadeBuysL20NotProd()
    str_join_b,   str_join_a ='','' 
    if len(str_dna) >0:
        str_join_b = str_join.format(table=table_b.format(str_l21=l21), filter_personas=str_dna)
        str_join_a = str_join.format(table=table_a, filter_personas=str_dna)

    query_b = query.format(str_l21=l21,condition=str_un,
                            program=loyalty_program, regions=regions,un_code=un_code,
                            objective='', filters=str_filters, joins_a=str_join_a,joins_b=str_join_b)    


    return query_b
    
def getQueryDoesntBuyL20(l21,l20,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters):

    query, table = Queries().LealdadeDoesntBuyl20()

    if len(str_dna) >0:
        str_join = str_join.format(table=table, filter_personas=str_dna)

    query = query.format(condition=str_un, filters=str_filters, un_code=un_code,
                            program=loyalty_program, regions=regions, joins=str_join)
        
       
    return query

def getQuerySpendMostloyaty10(l30,target_code,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters):
    """
        Lealdade 6 gasta mais com meu produto usado na lealdade 10
    """
    query, table = Queries().LealdadeSpendMostMyProductLoyalty10()
    queries = []
    str_join_item = ""

    
    if  loyalty_program =="AND loyalty_program_code in ('3')":
        loyalty_program = ""

    for l30_item in l30:
   
        
        if len(str_dna) >0:
            str_join_item = str_join.format(table=table.format(l30=l30_item), filter_personas=str_dna)

        queries.append(query.format(l30=l30_item, filters=str_filters,
                                program=loyalty_program, regions=regions,
                                joins=str_join_item,loyalty=''))

    final_queries = "select count(distinct prsn_id) from ({queries})".format(queries =" union ".join(queries) )   
    return final_queries


def getQuerySpendMost(l30,target_code,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters):
    """
        Lealdade 6 gasta mais com meu produto
    """
    query, table = Queries().LealdadeSpendMostMyProduct()
    queries = []
    str_join_item = ""

    if target =='SUB-CATEGORIA':
        loyalty = 'is_loyal_l30'
    elif target =='CATEGORIA':
        loyalty = 'is_loyal_l40'
    else:
        loyalty = 'is_loyal_b'
        
    if  loyalty_program =="AND loyalty_program_code in ('3')":
        loyalty_program = ""

    for l30_item in l30:
   
        
        if len(str_dna) >0:
            str_join_item = str_join.format(table=table.format(l30=l30_item), filter_personas=str_dna)

        queries.append(query.format(l30=l30_item, filters=str_filters,
                                loyal='true', program=loyalty_program, regions=regions,
                                joins=str_join_item,loyalty=loyalty))

    final_queries = "select count(distinct t.prsn_id) from ({queries}) AS T;".format(queries =" union ".join(queries) )   
    return final_queries

def getQueryDoesntSpendMost(l30,target_code,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters):
    
    """
        Lealdade 7 Gasta mais com produtos concorrentes do que com meu produto
    """
    query, table = Queries().LealdadeSpendMostMyProduct()
    queries = []
    str_join_item = ""

    if target =='SUB-CATEGORIA':
        loyalty = 'is_loyal_l30'
    elif target =='CATEGORIA':
        loyalty = 'is_loyal_l40'
    else:
        loyalty = 'is_loyal_b'

    if  loyalty_program =="AND loyalty_program_code in ('3')":
        loyalty_program = ""

    for l30_item in l30:

        
        if len(str_dna) >0:
            str_join_item = str_join.format(table=table.format(l30=l30_item), filter_personas=str_dna)

        queries.append(query.format(l30=l30_item, filters=str_filters,
                                loyal='false', program=loyalty_program, regions=regions,
                                joins=str_join_item,loyalty=loyalty))

    final_query = "select count(distinct t.prsn_id) from ({queries}) AS T;".format(queries =" union ".join(queries) )   
    return final_query

def getQueryLaunchProduct(l30,target_code,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters):
    """
        Lealdade 10 Compra lancamentos e minha categoria
    """
    query, table = Queries().LealdadeLaunchProduct()
    queries = []
    str_join_item = ""

   

    
    
    if len(str_dna) >0:
        str_join_item = str_join.format(table=table, filter_personas=str_dna)

    queries.append(query.format(lcode=target_code, filters=str_filters,
                            program=loyalty_program, regions=regions, joins=str_join_item))

    subtract_query = getQuerySpendMostloyaty10(l30,target_code,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters)
    
    final_query = "SELECT (select sum(qty) from ({queries}) as t ) - ({subquery} as d) as c".format(queries =" union ".join(queries),subquery=subtract_query )   
    return final_query


def getQueryRecover(l30,target_code,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters):
    
    """
        Lealdade 20 Compra lancamentos e minha categoria
    """
    query, table =  Queries().LealdadeRecoverProduct()
    #query = query.format(l21=l21,
    #                        filters=str_filters, loyal='true',
    #                        program=loyalty_program, regions=regions,
    #                        joins=str_join)
                        
    queries = []
    str_join_item = ""

    if target =='SUB-CATEGORIA':
        loyalty = 'is_loyal_l30'
    elif target =='CATEGORIA':
        loyalty = 'is_loyal_l40'
    else:
        loyalty = 'is_loyal_b'

    if  loyalty_program =="AND loyalty_program_code in ('3')":
        loyalty_program == ""

    for l30_item in l30:

        
        if len(str_dna) >0:
            str_join_item = str_join.format(table=table.format(l30=l30_item), filter_personas=str_dna)

        queries.append(query.format(l30=l30_item, filters=str_filters,
                                loyal='true', program=loyalty_program, regions=regions,
                                joins=str_join_item,loyalty=loyalty ))

    final_query = "select count(distinct t.prsn_id) from ({queries}) AS T;".format(queries =" union ".join(queries) )   
    return final_query

def getQueryDoesntBuyProduct(l30,l20,target,str_un,un_code,str_filters,str_dna,loyalty_program,regions,str_join,filters):
    
    """
        lealdade 5
    """
    operator = ' AND '
    if 'operador' in filters:
        operator = filters['operador']

   # filters_l30_table = str_filters.replace('AND ', 'AND t_audience_l21_{l30}.'.format(l30=l30))
   # loyalty_program_code_l30_table = loyalty_program.replace('AND ', 'AND t_audience_l21_{l30}.'.format(l30=l30))
   # regions_l30_table = regions.replace('AND ', 'AND t_audience_l21_{l30}.'.format(l30=l30))

    filters_un_table = str_filters.replace('AND ', 'AND u.')
    loyalty_program_code_un_table = loyalty_program.replace('AND ', 'AND u.')
    regions_un_table = regions.replace('AND ', 'AND u.')

    

    if not str_un and not len(str_dna) >0:
        audienceQuery = getQueryAudiencenoUN(l30,target,str_filters,str_dna,loyalty_program,regions,str_join)
        query = Queries().LealdadeDoesntBuyProductNoUN()[0]
        query = query.format(
                            l30=l30,
                            filters=str_filters,
                            filters_un=str_filters[4:],
                            loyalty_program=loyalty_program,
                            regions=regions,
                            audience = audienceQuery
                            )

        
        query = query.lower().replace('where  and', 'where').replace('where   and', 'where').replace('where    and', 'where')

    else:

        un = ''
        if str_un:
            un = str_un[4:len(str_un)]
            un = un.replace('and', operator)
            un = 'AND ({un}) '.format(un=un)

        str_join_un=""
        if len(str_dna) >0:
            str_join_un = str_join.format(table='u', filter_personas=str_dna)

        audienceQuery = getQueryAudienceUN(l30,target,str_filters,str_dna,loyalty_program,regions,str_join)
        query = Queries().LealdadeDoesntBuyProductUN()
        query = query[0].format(
            condition=un,
            filters_un_table=filters_un_table,            
            loyalty_program_code_un_table=loyalty_program_code_un_table,            
            regions_un_table=regions_un_table,            
            join_persona=str_join_un,
            audience=audienceQuery
        )
    # print query

    return query
   
# Returns a string to be used in the WHERE clause to query using the filters present in audience_conf
def build_str_gender_age_prc_sens_queries(audience_conf):
    if 'filtros' not in audience_conf:
        return ''

    filter_genero = audience_conf['filtros'].get('genero')
    filter_age = audience_conf['filtros'].get('idade')

    query = ''

    # We filter by gender only if it's available and it's not 2, that is, both genders
    if filter_genero and len(filter_genero) < 2:
        query += ' AND prsn_gender_code in {gendercode}' \
            .format(gendercode=listdecode2str(gender_func, audience_conf['filtros']['genero']))

    # We filter by age only if it's available and it's not 6, that is, all age ranges available
    if filter_age and len(filter_age) < 6:
        query += ' AND prsn_age_range_code in {agecode}' \
            .format(agecode=numlist2str(audience_conf['filtros']['idade']))

    if audience_conf['filtros'].get('sensibilidadePreco') != "0":
        query += ' AND prsn_price_sens_code in {pricecode}' \
            .format(pricecode=listdecode2str(sens_func, [audience_conf['filtros']['sensibilidadePreco']]))
    # Important: Here we assume that sensibilidadePreco = 0 is the same as no filter at all regarding price sensibility

    #We filter if client agreed to receive push notifications    
    if audience_conf['filtros'].get('metodo_envio') in [0,1]:
        query += ' AND prsn_push_optin_flag = {0}'.format(audience_conf['filtros'].get('metodo_envio'))

    return query

    

    

def getQueryAudiencenoUN(l30,target,str_filters,str_dna,loyalty_program,regions,str_join):
    """
        get audience  query for loyalty 5 without UN
    """
    query, table =  Queries().LealdadeDoesntBuyProductAudience()
    queries = []
    str_join_item = ""

    if target =='SUB-CATEGORIA':
        loyalty = 'is_loyal_l30'
    elif target =='CATEGORIA':
        loyalty = 'is_loyal_l40'
    else:
        loyalty = 'is_loyal_b'

    if  loyalty_program =="AND loyalty_program_code in ('3')":
        loyalty_program == ""

    for l30_item in l30:

        
        if len(str_dna) >0:
            str_join_item = str_join.format(table=table.format(l30=l30_item), filter_personas=str_dna)

        queries.append(query.format(l30=l30_item, filters=str_filters,
                                program=loyalty_program, regions=regions,
                                joins=str_join_item,loyalty=loyalty))

    final_queries = " select count(*) from ({queries}) AS T ".format(queries =" union ".join(queries) )   
    return final_queries
        

def getQueryAudienceUN(l30,target,str_filters,str_dna,loyalty_program,regions,str_join):
    """
        get audience  query for loyalty 5 with UN
    """
    query, table =  Queries().LealdadeDoesntBuyProductAudience()
    queries = []
    str_join_item = ""

    if target =='SUB-CATEGORIA':
        loyalty = 'is_loyal_l30'
    elif target =='CATEGORIA':
        loyalty = 'is_loyal_l40'
    else:
        loyalty = 'is_loyal_b'

    if  loyalty_program =="AND loyalty_program_code in ('3')":
        loyalty_program == ""


    for l30_item in l30:

        
        if len(str_dna) >0:
            str_join_item = str_join.format(table=table.format(l30=l30_item), filter_personas=str_dna)

        queries.append(query.format(l30=l30_item, filters=str_filters,
                                program=loyalty_program, regions=regions,
                                joins=str_join_item,loyalty=loyalty))

    final_queries = " {queries} ".format(queries =" union ".join(queries) )   
    return final_queries
