import datetime
import pandas as pd
import requests as rq 
import numpy as np
from localidades import estadual, municipal
import ssl
import openpyxl
from ajustar_planilha import ajustar_bordas, ajustar_colunas
from openpyxl.styles import Font, Border, Side
from requests.adapters import HTTPAdapter
# from googleapiclient.http import MediaFileUpload
# from Drive import add_arquivos_a_pasta

#TABELA
tabela289 = 289
tabela291 = 291
tabela5930 = 5930


lista_cidades_mt = [5100102,5100201,5100250,5100300,5100359,5100409,5100508,5100607,5100805,5101001,5101209,5101258,5101308,5101407,5101605,5101704,5101803,5101852,5101902,5102504,5102603,5102637,5102678,5102686,5102694,5102702,5102793,5102850,5103007,5103056,5103106,5103205,5103254,5103304,5103353,5103361,5103379,5103403,5103437,5103452,5103502,5103601,5103700,5103809,5103858,5103908,5103957,5104104,5104203,5104500,5104526,5104542,5104559,5104609,5104807,5104906,5105002,5105101,5105150,5105176,5105200,5105234,5105259,5105309,5105507,5105580,5105606,5105622,5105903,5106000,5106109,5106158,5106174,5106182,5106190,5106208,5106216,5106224,5106232,5106240,5106257,5106265,5106273,5106281,5106299,5106307,5106315,5106372,5106422,5106455,5106505,5106653,5106703,5106752,5106778,5106802,5106828,5106851,5107008,5107040,5107065,5107107,5107156,5107180,5107198,5107206,5107248,5107263,5107297,5107305,5107354,5107404,5107578,5107602,5107701,5107743,5107750,5107768,5107776,5107792,5107800,5107859,5107875,5107883,5107909,5107925,5107941,5107958,5108006,5108055,5108105,5108204,5108303,5108352,5108402,5108501,5108600,5108808,5108857,5108907,5108956]
lista_cod_produto = [3402,3405,3408,39409,3411,3412,3416,3418,3433,3434,3435,3438,3439,3440,3444]
outros_map = {
    '11296': 'Outros alimentícios',
    '3415': 'Outros aromáticos, medicinais, tóxicos e corantes',
    '110011': 'Outras ceras',
    '3427': 'Outras fibras',
    '3446': 'Outros oleaginosos',
    '3454': 'Outros tanantes'
}

nomes_produtos = [
    "Lenha de eucalipto",
    "Lenha de pinus",
    "Lenha de outras espécies",
    "Madeira em tora para papel e celulose",
    "Madeira em tora de eucalipto para papel e celulose",
    "Madeira em tora de pinus para papel e celulose",
    "Madeira em tora de outras espécies para papel e celulose",
    "Madeira em tora para outras finalidades",
    "Madeira em tora de eucalipto para outras finalidades",
    "Madeira em tora de pinus para outras finalidades",
    "Madeira em tora de outras espécies para outras finalidades"
]

class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT

        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)
    
def requisitando_dados(api):
    with rq.Session() as s:
        s.mount("https://", TLSAdapter())
        try:
            response = s.get(api, verify=True)
            if response.status_code != 200:
                raise Exception(f"A solicitação à API falhou com o código de status: {response.status_code}")
            return response.json()
        except rq.exceptions.RequestException as e:
            raise Exception(f"Erro na solicitação: {e}")
    
def extrair_dados(api, tabela_id):
    dados_brutos = requisitando_dados(api)

    variaveis_por_tabela = {
        289: ['variavel_144', 'variavel_145'],
        291: ['variavel_142', 'variavel_143'],
        5930: ['variavel_6549']
    }

    if tabela_id in variaveis_por_tabela:
        variaveis = variaveis_por_tabela[tabela_id]
        
        if dados_brutos:
            resultado = [dados_brutos[i] if i < len(dados_brutos) else None for i in range(len(variaveis))]
        else:
            resultado = [None] * len(variaveis)
        
   
        return resultado[0] if len(variaveis) == 1 else tuple(resultado)

    return None

def tratando_tabela291(variavel142, variavel143):
    dados_limpos_142 = []
    dados_limpos_143 = []
    lista_produto = []

    variaveis = [variavel142, variavel143]

    for i in variaveis:
        id_tabela = i['id']
        variavel = i['variavel']
        unidade = i['unidade']
        dados = i['resultados']

        for ii in dados:
            dados_produto = ii['classificacoes']
            dados_producao = ii['series']
            
            for iii in dados_produto:
                dados_id_produto = iii['categoria']

                for id_produto, nome_produto in dados_id_produto.items():
                    nome_produto = nome_produto.split('-')[1].strip()
                    
                    for iv in dados_producao:
                        id = iv['localidade']['id']
                        nome = iv['localidade']['nome'].replace(' (MT)', '')
                        dados_ano_producao = iv['serie']

                        for ano, producao in dados_ano_producao.items():
                            producao = producao.replace('-', '0').replace('...', '0')
                           
                            dict = {
                                'id': id,
                                'nome': nome,
                                'produto': nome_produto,
                                variavel: producao,
                                'unidade': unidade,
                                'ano': f'01/01/{ano}'
                            }
                            if id_tabela == '142':
                                dados_limpos_142.append(dict)
                            else:
                                dados_limpos_143.append(dict)


    return dados_limpos_142, dados_limpos_143

def tratando_tabela5930(variavel6549):
    dados_limpos_6549 = []
    lista_especies = []

    id_tabela =  variavel6549['id']
    unidade = variavel6549['unidade']
    variavel = variavel6549['variavel']
    dados = variavel6549['resultados']

    for i in dados:
        dados_produto = i['classificacoes']
        dados_producao =  i['series']
        
        for ii in dados_produto:
            dados_id_especie =  ii['categoria']
                
            for id_especie, nome_especie in dados_id_especie.items():
                
                for iii in dados_producao:
                    id = iii['localidade']['id']
                    nome = iii['localidade']['nome'].replace(' (MT)', '')
                    dados_ano_producao = iii['serie']

                    for ano, producao in dados_ano_producao.items():
                        producao = producao.replace('-', '0').replace('...', '0')

                        dict = { 
                            'id': id,
                            'nome': nome,
                            'id_especie': id_especie,
                            'especie': nome_especie,
                            variavel: producao,
                            'unidade': unidade,
                            'ano': f'01/01/{ano}'
                        }
                        dados_limpos_6549.append(dict)


    return dados_limpos_6549

def tratando_tabela289(variavel144, variavel145):
    dados_limpos_144 = []
    dados_limpos_145 = []

    variaveis = [variavel144, variavel145]

    for i in variaveis:
        id_tabela = i['id']
        variavel = i['variavel']
        unidade = i['unidade']
        dados = i['resultados']

        for ii in dados:
            dados_produto = ii['classificacoes']
            dados_producao = ii['series']
            
            for iii in dados_produto:
                dados_id_produto = iii['categoria']

                for id_produto, nome_produto in dados_id_produto.items():
                    nome_produto = nome_produto.split('-', 1)[1].strip() if '-' in nome_produto else nome_produto.strip()
                    numero_produto = id_produto.strip()
                    
                    # Renomeia "Outros/Outras" conforme o grupo
                    if numero_produto in outros_map:
                        nome_produto = outros_map[numero_produto]

                    nome_produto = nome_produto.replace('Pequi (fruto)', 'Pequi')\
                        .replace('Ipecacuanha ou poaia (raiz)', 'Ipecacuanha ou poaia')\
                        .replace('Hevea (látex coagulado)', 'Hevea látex coagulado')\
                        .replace('Copaíba (óleo)', 'Copaíba')\
                        .replace('Pequi (amêndoa)', 'Pequi amêndoa')\
                        .replace('Babaçu (amêndoa)', 'Babaçu').replace('Açaí (fruto)', 'Açai').replace('Mangaba (fruto)', 'Mangaba')\
                        .replace('Umbu (fruto)', 'Umbu').replace('Jaborandi (folha)', 'Jaborandi').replace('Urucum (semente)', 'Urucum')\
                        .replace('Hevea (látex líquido)', 'Hevea látex líquido').replace('Carnaúba (cera)', 'Carnaúba cera').replace('Carnaúba (pó)', 'Carnaúba')\
                        .replace('Cumaru (amêndoa)', 'Cumaru').replace('Licuri (coquilho)', 'Licuri').replace('Tucum (amêndoa)', 'Tucum')\
                        .replace('Pinheiro brasileiro (nó de pinho)', 'Pinheiro brasileiro nó de pinho').replace('Pinheiro brasileiro (árvores abatidas)', 'Pinheiro brasileiro árvores abatidas')\
                        .replace('Pinheiro brasileiro (madeira em tora)', 'Pinheiro brasileiro em maderia em tora').replace('Angico (casca)', 'Angico casca')\
                        .replace('Barbatimão (casca)', 'Barbatimão').replace('Oiticica (semente)', 'Oiticica')
                    
                    for iv in dados_producao:
                        id = iv['localidade']['id']
                        nome = iv['localidade']['nome'].replace(' (MT)', '')
                        dados_ano_producao = iv['serie']

                        for ano, producao in dados_ano_producao.items():
                            producao = producao.replace('-', '0').replace('...', '0')
                            dict = {
                                'id': id,
                                'nome': nome,
                                'produto': nome_produto,
                                variavel: producao,
                                'unidade': unidade,
                                'ano': f'01/01/{ano}'
                            }
                            if id_tabela == '144':
                                dados_limpos_144.append(dict)
                            else:
                                dados_limpos_145.append(dict)
    return dados_limpos_144, dados_limpos_145

    
# def executando_funcoes():
#     variavel142estadual, variavel143estadual = extrair_dados(api291estadual, tabela291)
#     variavel142municipal, variavel143municipal = extrair_dados(api291municipal, tabela291)


#     dados_limpos_142_estadual, dados_limpos_143_estadual, lista_produtos = tratando_tabela291(variavel142estadual, variavel143estadual)
#     dados_limpos_142_municipal, dados_limpos_143_municipal, lista_produtos = tratando_tabela291(variavel142municipal, variavel143municipal)
    
#     variavel6549estadual = extrair_dados(api5930estadual, tabela5930)
#     variavel6549municipal = extrair_dados(api5930municipal, tabela5930)

#     dados_limpos_6549_estadual, lista_especies = tratando_tabela5930(variavel6549estadual)
#     dados_limpos_6549_municipal, lista_especies = tratando_tabela5930(variavel6549municipal)
    
#     variavel144estadual, variavel145estadual = extrair_dados(api289estadual, tabela289)
    
#     dados_limpos_144_estadual, dados_limpos_145_estadual = tratando_tabela289(variavel144estadual, variavel145estadual)
    

#     return dados_limpos_142_estadual, dados_limpos_142_municipal, dados_limpos_143_estadual, dados_limpos_143_municipal,  \
#         dados_limpos_6549_estadual, dados_limpos_6549_municipal, lista_produtos, lista_especies, dados_limpos_144_estadual, dados_limpos_145_estadual
        
ano_atual = datetime.datetime.now().year
def executando(tabela, tipo):
    lista_dados_144 = []
    lista_dados_145 = []
    lista_dados_142 = []
    lista_dados_143 = [] 
    lista_dados_6549 = []
    for ano in range(2014, ano_atual+1):  # incluir ano atual
        if tabela == 289:
            if tipo == 'estadual':
                api =  f'https://servicodados.ibge.gov.br/api/v3/agregados/289/periodos/{ano}/variaveis/144|145?{estadual}&classificacao=193[3403,3404,3405,3406,3407,3408,39409,3409,3410,11296,3412,3413,3414,3415,3417,3418,3419,40524,3421,3422,110011,3424,3425,3426,3427,3429,3430,3431,3433,3434,3435,3439,3440,3441,3442,3443,3444,3445,3446,3448,3449,3450,3452,3453,3454]'
                variavel_144, variavel_145 = extrair_dados(api, tabela)
                if variavel_144 is None and variavel_145 is None:
                    continue
                novos_dados_144, novos_dados_145 = tratando_tabela289(variavel_144, variavel_145)
                lista_dados_144.extend(novos_dados_144)
                lista_dados_145.extend(novos_dados_145)
            else:
                for cidade in lista_cidades_mt:
                    api = f'https://servicodados.ibge.gov.br/api/v3/agregados/289/periodos/{ano}/variaveis/144|145?localidades=N6[{cidade}]&classificacao=193[3403,3404,3405,3406,3407,3408,39409,3409,3410,11296,3412,3413,3414,3415,3417,3418,3419,40524,3421,3422,110011,3424,3425,3426,3427,3429,3430,3431,3433,3434,3435,3439,3440,3441,3442,3443,3444,3445,3446,3448,3449,3450,3452,3453,3454]'
                    # opcional: print(api)
                    variavel_144, variavel_145 = extrair_dados(api, tabela)
                    if variavel_144 is None and variavel_145 is None:
                        continue
                    novos_dados_144, novos_dados_145 = tratando_tabela289(variavel_144, variavel_145)
                    lista_dados_144.extend(novos_dados_144)
                    lista_dados_145.extend(novos_dados_145)
        
        elif tabela == 291:
            if tipo == 'estadual':
                api = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela291}/periodos/{ano}/variaveis/142|143?{estadual}&classificacao=194[33247,33248,33249,33250,33251,33252,33253,33254,33255,33256,33257,33258,3461,3462,3463]'
            else:
                api = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela291}/periodos/{ano}/variaveis/142|143?{municipal}&classificacao=194[33247,33248,33249,33250,33251,33252,33253,33254,33255,33256,33257,33258,3461,3462,3463]'
            variavel_142, variavel_143 = extrair_dados(api, tabela)
            if variavel_142 == None and variavel_143 == None:
                break
            else:
                novos_dados_142, novos_dados_143 = tratando_tabela291(variavel_142, variavel_143)
                lista_dados_142.extend(novos_dados_142)
                lista_dados_143.extend(novos_dados_143)

        elif tabela == 5930:
            if tipo == 'estadual':
                api = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela5930}/periodos/{ano}/variaveis/6549?{estadual}&classificacao=734[39326,39327,39328]'
            else:
                api= f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela5930}/periodos/{ano}/variaveis/6549?{municipal}&classificacao=734[39326,39327,39328]' 
                
            variavel_6549 = extrair_dados(api, tabela)
            
            if variavel_6549 == None:
                break
            else:
                novos_dados_6549 = tratando_tabela5930(variavel_6549)
                lista_dados_6549.extend(novos_dados_6549)

    if tabela == 289:
        return  lista_dados_144, lista_dados_145
    elif tabela == 291:
        return  lista_dados_142, lista_dados_143
    elif tabela == 5930:
        return  lista_dados_6549
    else:
        return 'dados não existentes'


def gerando_dataframe_291(dados_limpos_142_estadual, dados_limpos_142_municipal, dados_limpos_143_estadual, dados_limpos_143_municipal):
    df142estadual = pd.DataFrame(dados_limpos_142_estadual)
    df142municipal = pd.DataFrame(dados_limpos_142_municipal)
    

    df143estadual = pd.DataFrame(dados_limpos_143_estadual)
    df143municipal = pd.DataFrame(dados_limpos_143_municipal)
    
    
    df291estadual = pd.merge(df142estadual, df143estadual, on=['id', 'nome', 'produto',  'ano'], how='inner')
    df291estadual = df291estadual.rename(columns={"unidade_x": "unidade_quantidade", "unidade_y": "unidade_producao"})
    df291estadual['Quantidade produzida na silvicultura'] = df291estadual['Quantidade produzida na silvicultura'].astype(float)
    df291estadual['Valor da produção na silvicultura'] = df291estadual['Valor da produção na silvicultura'].astype(float)
    df291estadual['Valor da produção na silvicultura'] = df291estadual['Valor da produção na silvicultura'] * 1000
    
    df291estadual['unidade_quantidade'] = df291estadual['produto'].apply(lambda x: 'metros cubicos' if x in nomes_produtos else 'toneladas')

    
    df291municipal = pd.merge(df142municipal, df143municipal, on=['id', 'nome',  'produto',  'ano'], how='inner')
    df291municipal = df291municipal.rename(columns={"unidade_x": "unidade_quantidade", "unidade_y": "unidade_producao"})
    df291municipal['Quantidade produzida na silvicultura'] = df291municipal['Quantidade produzida na silvicultura'].astype(float)
    df291municipal['Valor da produção na silvicultura'] = df291municipal['Valor da produção na silvicultura'].astype(float)
    df291municipal['Valor da produção na silvicultura'] = df291municipal['Valor da produção na silvicultura'] * 1000
    df291municipal['unidade_quantidade'] = df291municipal['produto'].apply(lambda x: 'metros cubicos' if x in nomes_produtos else 'toneladas')

    return df291estadual, df291municipal

def gerando_dataframe_5930(dados_limpos_6549_estadual, dados_limpos_6549_municipal):
    df6549estadual =  pd.DataFrame(dados_limpos_6549_estadual)
    df6549municipal = pd.DataFrame(dados_limpos_6549_municipal)


    df6549estadual['Área total existente em 31/12 dos efetivos da silvicultura'] = df6549estadual['Área total existente em 31/12 dos efetivos da silvicultura'].astype(float)
    df6549municipal['Área total existente em 31/12 dos efetivos da silvicultura'] = df6549municipal['Área total existente em 31/12 dos efetivos da silvicultura'].astype(float)
    return df6549estadual, df6549municipal


def gerando_dataframe289(dados_limpos_144_estadual, dados_limpos_145_estadual, dados_limpos_144_municipal, dados_limpos_145_municipal):
    df144estadual = pd.DataFrame(dados_limpos_144_estadual)
    df144municipal = pd.DataFrame(dados_limpos_144_municipal)
    
    df145estadual = pd.DataFrame(dados_limpos_145_estadual)
    df145municipal = pd.DataFrame(dados_limpos_145_municipal)
    
    df289estadual = pd.merge(df144estadual, df145estadual, on=['id', 'nome', 'produto', 'ano'], how='inner')
    df289municipal = pd.merge(df144municipal, df145municipal, on=['id', 'nome', 'produto', 'ano'], how='inner')
    
    df289estadual = df289estadual.rename(columns={"unidade_x": "unidade_quantidade", "unidade_y": "unidade_producao"})
    df289municipal = df289municipal.rename(columns={"unidade_x": "unidade_quantidade", "unidade_y": "unidade_producao"})
    
    df289estadual.loc[(df289estadual['produto'].isin(['Lenha', 'Madeira em tora'])) & (df289estadual['unidade_quantidade'] == 'Toneladas'),'unidade_quantidade'] = 'Metros Cúbicos'
    df289municipal.loc[(df289municipal['produto'].isin(['Pequi'])) & (df289municipal['unidade_quantidade'] == ''),'unidade_quantidade'] = 'Toneladas'

    df289estadual['Quantidade produzida na extração vegetal'] = df289estadual['Quantidade produzida na extração vegetal'].astype(float)
    df289estadual['Valor da produção na extração vegetal'] = df289estadual['Valor da produção na extração vegetal'].astype(float)
    df289estadual['Valor da produção na extração vegetal'] = df289estadual['Valor da produção na extração vegetal'] * 1000
    
    df289municipal['Quantidade produzida na extração vegetal'] = df289municipal['Quantidade produzida na extração vegetal'].astype(float)
    df289municipal['Valor da produção na extração vegetal'] = df289municipal['Valor da produção na extração vegetal'].astype(float)
    df289municipal['Valor da produção na extração vegetal'] = df289municipal['Valor da produção na extração vegetal'] * 1000
    
    df289municipal['produto'] = df289municipal['produto']
    
    return df289estadual, df289municipal

#PEVS (ESTADUAL E MUNICIPAL)
dados_limpos_144_estadual, dados_limpos_145_estadual  = executando(289, 'estadual')
dados_limpos_144_municipal, dados_limpos_145_municipal  = executando(289, 'municipal')

dados_limpos_142_estadual, dados_limpos_143_estadual  = executando(291, 'estadual')
dados_limpos_142_municipal, dados_limpos_143_municipal  = executando(291, 'municipal')

dados_limpos_6549_estadual = executando(5930, 'estadual')
dados_limpos_6549_municipal = executando(5930, 'municipal')

df289estadual, df289municipal = gerando_dataframe289(dados_limpos_144_estadual, dados_limpos_145_estadual, dados_limpos_144_municipal, dados_limpos_145_municipal)
df291estadual, df291municipal = gerando_dataframe_291(dados_limpos_142_estadual, dados_limpos_142_municipal, dados_limpos_143_estadual, dados_limpos_143_municipal)
df6549estadual, df6549municipal = gerando_dataframe_5930(dados_limpos_6549_estadual, dados_limpos_6549_municipal)


print(df289estadual.head())
#ABRE O ARQUIVO SQL.PY E EXECUTA TODOS OS COMANDOS DENTRO DELE
if __name__ == '__main__':
    from sql import executar_sql  # Importe a função aqui para evitar o erro de importação cíclica
    executar_sql()

