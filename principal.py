import pandas as pd
import requests as rq 
import numpy as np
import pprint
import sqlite3
from localidades import nacional, estadual, municipal
import ssl
import time
from Google import Create_Service
from googleapiclient.http import MediaFileUpload
import openpyxl
from ajustar_planilha import ajustar_bordas, ajustar_colunas
from openpyxl.styles import Font, Border, Side

#TABELA
tabela289 = 289
tabela291 = 291
tabela5930 = 5930


#VARIAVEL
api289nacional = f'https://servicodados.ibge.gov.br/api/v3/agregados/289/periodos/2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/144|145?{nacional}&classificacao=193[3402,3405,3408,39409,3411,3412,3416,3418,3433,3434,3435,3438,3439,3440,3444]'
api289estadual = f'https://servicodados.ibge.gov.br/api/v3/agregados/289/periodos/2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/144|145?{estadual}&classificacao=193[3402,3405,3408,39409,3411,3412,3416,3418,3433,3434,3435,3438,3439,3440,3444]'
#api289municipal =f'https://servicodados.ibge.gov.br/api/v3/agregados/289/periodos/2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/144|145?{municipal}&classificacao=193[3402,3405,3408,39409,3411,3412,3416,3418,3433,3434,3435,3438,3439,3440,3444]'

api291nacional = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela291}/periodos/2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/142|143?{nacional}&classificacao=194[3455,33247,33248,33249,3456,33250,33251,33252,3457,3458,33253,33254,33255,3459,33256,33257,33258]'
api291estadual = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela291}/periodos/2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/142|143?{estadual}&classificacao=194[3455,33247,33248,33249,3456,33250,33251,33252,3457,3458,33253,33254,33255,3459,33256,33257,33258]'
api291municipal = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela291}/periodos/2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/142|143?{municipal}&classificacao=194[3455,33247,33248,33249,3456,33250,33251,33252,3457,3458,33253,33254,33255,3459,33256,33257,33258]'

api5930nacional = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela5930}/periodos/2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/6549?{nacional}&classificacao=734[39326,39327,39328]' 
api5930estadual = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela5930}/periodos/2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/6549?{estadual}&classificacao=734[39326,39327,39328]'
api5930municipal = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela5930}/periodos/2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/6549?{municipal}&classificacao=734[39326,39327,39328]' 

class TLSAdapter(rq.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.options |= 0x4   # OP_LEGACY_SERVER_CONNECT
        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

def requisitando_dados(api):
    with rq.session() as s:
        s.mount("https://", TLSAdapter())
        dados_brutos_api = s.get(api, verify=True)
        return dados_brutos_api.json()
    
    if dados_brutos_api.status_code != 200:
        raise Exception(f"A solicitação à API falhou com o código de status: {dados_brutos_api.status_code}")
    

def extrair_dados(api, tabela_id):
    dados_brutos = requisitando_dados(api)
    
    if dados_brutos:
        if tabela_id == tabela291:
            variavel142 = dados_brutos[0]
            variavel143 = dados_brutos[1]
            return variavel142, variavel143
        
        elif tabela_id == tabela5930:
            variavel6549 = dados_brutos[0]
            return variavel6549
        elif tabela_id == tabela289:
            variavel144 = dados_brutos[0]
            variavel145 = dados_brutos[1]
            return variavel144, variavel145
    else:
        pass

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

                            dict2 = {
                                'id_produto': id_produto,
                                'nome_produto': nome_produto
                            }
                            lista_produto.append(dict2)

    return dados_limpos_142, dados_limpos_143, lista_produto

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

                        dict2 = {
                                'id_especie': id_especie,
                                'nome_especie': nome_especie
                        }
                        lista_especies.append(dict2)

    return dados_limpos_6549, lista_especies

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
                    nome_produto = nome_produto.split('-')[1].strip()
                    nome_produto = nome_produto.replace('Pequi (fruto)', 'Pequi')\
                    .replace('Ipecacuanha ou poaia (raiz)', 'Ipecacuanha ou poaia')\
                    .replace('Hevea (látex coagulado)', 'Hevea látex coagulado')\
                    .replace('Copaíba (óleo)', 'Copaíba')\
                    .replace('Pequi (amêndoa)', 'Pequi amêndoa')\
                    .replace('Babaçu (amêndoa)', 'Babaçu')
                    
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
    

def executando_funcoes():
    variavel142nacional, variavel143nacional = extrair_dados(api291nacional, tabela291)
    variavel142estadual, variavel143estadual = extrair_dados(api291estadual, tabela291)
    variavel142municipal, variavel143municipal = extrair_dados(api291municipal, tabela291)


    dados_limpos_142_nacional, dados_limpos_143_nacional, lista_produtos = tratando_tabela291(variavel142nacional, variavel143nacional)
    dados_limpos_142_estadual, dados_limpos_143_estadual, lista_produtos = tratando_tabela291(variavel142estadual, variavel143estadual)
    dados_limpos_142_municipal, dados_limpos_143_municipal, lista_produtos = tratando_tabela291(variavel142municipal, variavel143municipal)
    

    variavel6549nacional = extrair_dados(api5930nacional, tabela5930)
    variavel6549estadual = extrair_dados(api5930estadual, tabela5930)
    variavel6549municipal = extrair_dados(api5930municipal, tabela5930)

    dados_limpos_6549_nacional, lista_especies = tratando_tabela5930(variavel6549nacional)
    dados_limpos_6549_estadual, lista_especies = tratando_tabela5930(variavel6549estadual)
    dados_limpos_6549_municipal, lista_especies = tratando_tabela5930(variavel6549municipal)
    
    variavel144nacional, variavel145nacional = extrair_dados(api289nacional, tabela289)
    variavel144estadual, variavel145estadual = extrair_dados(api289estadual, tabela289)
    
    
    dados_limpos_144_nacional, dados_limpos_145_nacional = tratando_tabela289(variavel144nacional, variavel145nacional)
    dados_limpos_144_estadual, dados_limpos_145_estadual = tratando_tabela289(variavel144estadual, variavel145estadual)
    

    return dados_limpos_142_nacional, dados_limpos_142_estadual, dados_limpos_142_municipal, dados_limpos_143_nacional, dados_limpos_143_estadual, dados_limpos_143_municipal,  \
        dados_limpos_6549_nacional, dados_limpos_6549_estadual, dados_limpos_6549_municipal, lista_produtos, lista_especies, dados_limpos_144_nacional,  dados_limpos_144_estadual, dados_limpos_145_nacional, dados_limpos_145_estadual

def gerando_dataframe(dados_limpos_142_nacional, dados_limpos_142_estadual, dados_limpos_142_municipal, dados_limpos_143_nacional, dados_limpos_143_estadual, dados_limpos_143_municipal):
    df142nacional = pd.DataFrame(dados_limpos_142_nacional)
    df142estadual = pd.DataFrame(dados_limpos_142_estadual)
    df142municipal = pd.DataFrame(dados_limpos_142_municipal)
    
    df143nacional = pd.DataFrame(dados_limpos_143_nacional)
    df143estadual = pd.DataFrame(dados_limpos_143_estadual)
    df143municipal = pd.DataFrame(dados_limpos_143_municipal)
    
    df291nacional = pd.merge(df142nacional, df143nacional, on=['id', 'nome', 'produto', 'ano'], how='inner')
    df291nacional = df291nacional.rename(columns={"unidade_x": "unidade_quantidade", "unidade_y": "unidade_producao"})
    df291nacional['Quantidade produzida na silvicultura'] = df291nacional['Quantidade produzida na silvicultura'].astype(float)
    df291nacional['Valor da produção na silvicultura'] = df291nacional['Valor da produção na silvicultura'].astype(float)
    
    df291estadual = pd.merge(df142estadual, df143estadual, on=['id', 'nome', 'produto',  'ano'], how='inner')
    df291estadual = df291estadual.rename(columns={"unidade_x": "unidade_quantidade", "unidade_y": "unidade_producao"})
    df291estadual['Quantidade produzida na silvicultura'] = df291estadual['Quantidade produzida na silvicultura'].astype(float)
    df291estadual['Valor da produção na silvicultura'] = df291estadual['Valor da produção na silvicultura'].astype(float)
    
    df291municipal = pd.merge(df142municipal, df143municipal, on=['id', 'nome',  'produto',  'ano'], how='inner')
    df291municipal = df291municipal.rename(columns={"unidade_x": "unidade_quantidade", "unidade_y": "unidade_producao"})
    df291municipal['Quantidade produzida na silvicultura'] = df291municipal['Quantidade produzida na silvicultura'].astype(float)
    df291municipal['Valor da produção na silvicultura'] = df291municipal['Valor da produção na silvicultura'].astype(float)

    return df291nacional, df291estadual, df291municipal

def gerando_dataframe2(dados_limpos_6549_nacional, dados_limpos_6549_estadual, dados_limpos_6549_municipal):
    df6549nacional = pd.DataFrame(dados_limpos_6549_nacional)
    df6549estadual =  pd.DataFrame(dados_limpos_6549_estadual)
    df6549municipal = pd.DataFrame(dados_limpos_6549_municipal)


    df6549nacional['Área total existente em 31/12 dos efetivos da silvicultura'] = df6549nacional['Área total existente em 31/12 dos efetivos da silvicultura'].astype(float)
    df6549estadual['Área total existente em 31/12 dos efetivos da silvicultura'] = df6549estadual['Área total existente em 31/12 dos efetivos da silvicultura'].astype(float)
    df6549municipal['Área total existente em 31/12 dos efetivos da silvicultura'] = df6549municipal['Área total existente em 31/12 dos efetivos da silvicultura'].astype(float)
    return df6549nacional, df6549estadual, df6549municipal


def gerando_dataframe3(lista_produtos, lista_especies):
    df_produtos =  pd.DataFrame(lista_produtos)
    df_produtos = df_produtos.drop_duplicates()
    df_especies =  pd.DataFrame(lista_especies)
    df_especies = df_especies.drop_duplicates()

    return df_produtos, df_especies

def gerando_dataframe4(dados_limpos_144_nacional, dados_limpos_144_estadual, dados_limpos_145_nacional, dados_limpos_145_estadual):
    df144nacional = pd.DataFrame(dados_limpos_144_nacional)
    df144estadual = pd.DataFrame(dados_limpos_144_estadual)
    #df144municipal = pd.DataFrame(dados_limpos_144_municipal)
    
    df145nacional = pd.DataFrame(dados_limpos_145_nacional)
    df145estadual = pd.DataFrame(dados_limpos_145_estadual)
    #df145municipal = pd.DataFrame(dados_limpos_145_municipal)
    
    df289nacional = pd.merge(df144nacional, df145nacional, on=['id', 'nome', 'produto', 'ano'], how='inner')
    df289estadual = pd.merge(df144estadual, df145estadual, on=['id', 'nome', 'produto', 'ano'], how='inner')
    df289nacional = df289nacional.rename(columns={"unidade_x": "unidade_quantidade", "unidade_y": "unidade_producao"})
    df289estadual = df289estadual.rename(columns={"unidade_x": "unidade_quantidade", "unidade_y": "unidade_producao"})
    #df289municipal = pd.merge(df145municipal, df145municipal, on=['id', 'nome', 'produto', 'ano'], how='inner')
    df289nacional['Quantidade produzida na extração vegetal'] = df289nacional['Quantidade produzida na extração vegetal'].astype(float)
    df289nacional['Valor da produção na extração vegetal'] = df289nacional['Valor da produção na extração vegetal'].astype(float)
    
    df289estadual['Quantidade produzida na extração vegetal'] = df289estadual['Quantidade produzida na extração vegetal'].astype(float)
    df289estadual['Valor da produção na extração vegetal'] = df289estadual['Valor da produção na extração vegetal'].astype(float)
    
    return df289nacional, df289estadual

#PEVS 291 142/143 (NACIONAL, ESTADUAL E MUNICIPAL)
dados_limpos_142_nacional, dados_limpos_142_estadual, dados_limpos_142_municipal, dados_limpos_143_nacional, dados_limpos_143_estadual, dados_limpos_143_municipal, dados_limpos_6549_nacional, \
    dados_limpos_6549_estadual, dados_limpos_6549_municipal, lista_produtos, lista_especies, dados_limpos_144_nacional, dados_limpos_144_estadual, dados_limpos_145_nacional, dados_limpos_145_estadual = executando_funcoes()

df291nacional, df291estadual, df291municipal = gerando_dataframe(dados_limpos_142_nacional, dados_limpos_142_estadual, dados_limpos_142_municipal, dados_limpos_143_nacional, dados_limpos_143_estadual, dados_limpos_143_municipal)
df291nacional.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_NACIONAL.xlsx', index=False)
df291estadual.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_ESTADUAL.xlsx', index=False)
df291municipal.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_MUNICIPAL.xlsx', index=False)

df291nacional.to_html('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\CHATBOT\\Banco de dados Bot\\PEVS_291_NACIONAL.html', index=False)
df291estadual.to_html('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\CHATBOT\\Banco de dados Bot\\PEVS_291_ESTADUAL.html', index=False)
df291municipal.to_html('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\CHATBOT\\Banco de dados Bot\\PEVS_291_MUNICIPAL.html', index=False)

#print(df291municipal)
#print(df291nacional)
#print(df291estadual)

#PEVS 5930 6549 (NACIONAL, ESTADUAL E MUNICIPAL)
df6549nacional, df6549estadual, df6549municipal = gerando_dataframe2(dados_limpos_6549_nacional, dados_limpos_6549_estadual, dados_limpos_6549_municipal)
df6549nacional.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_NACIONAL.xlsx', index=False)
df6549estadual.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_ESTADUAL.xlsx', index=False)
df6549municipal.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_MUNICIPAL.xlsx', index=False)

df6549nacional.to_html('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\CHATBOT\\Banco de dados Bot\\PEVS_5930_6549_NACIONAL.html', index=False)
df6549estadual.to_html('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\CHATBOT\\Banco de dados Bot\\PEVS_5930_6549_ESTADUAL.html', index=False)
df6549municipal.to_html('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\CHATBOT\\Banco de dados Bot\\PEVS_5930_6549_MUNICIPAL.html', index=False)

#print(df6549nacional)

df_produtos, df_especies = gerando_dataframe3(lista_produtos, lista_especies)
df_produtos.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_lista_produtos.xlsx', index=False)
df_especies.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_lista_especies.xlsx', index=False)

df289nacional, df289estadual = gerando_dataframe4(dados_limpos_144_nacional, dados_limpos_144_estadual, dados_limpos_145_nacional, dados_limpos_145_estadual)
df289nacional.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_289_NACIONAL.xlsx', index=False)
df289estadual.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_289_ESTADUAL.xlsx', index=False)

df289nacional.to_html('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\CHATBOT\\Banco de dados Bot\\PEVS_289_NACIONAL.html', index=False)
df289estadual.to_html('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\CHATBOT\\Banco de dados Bot\\PEVS_289_ESTADUAL.html', index=False)

#CARREGANDO AS PLANILHAS PEVS 291 E FAZENDO ALTERAÇÕIES NELAS
wb_291_nacional = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_NACIONAL.xlsx")  
wb_291_estadual = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_ESTADUAL.xlsx")  
wb_291_municipal = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_MUNICIPAL.xlsx")  

ws_291_nacional = wb_291_nacional.active
ws_291_estadual = wb_291_estadual.active
ws_291_municipal = wb_291_municipal.active

lista_ws = [ws_291_nacional, ws_291_estadual, ws_291_municipal]
lista_wb = [wb_291_nacional, wb_291_estadual, wb_291_municipal]

for ws3, wb3 in zip(lista_ws, lista_wb):
    ajustar_colunas(ws3)
    ajustar_bordas(wb3)
    
        
# Salvar a planilha
wb_291_nacional.save('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_NACIONAL.xlsx')
wb_291_estadual.save('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_ESTADUAL.xlsx')
wb_291_municipal.save('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_MUNICIPAL.xlsx')

#CARREGANDO AS PLANILHAS PEVS 5930_6549
wb_5930_nacional = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_NACIONAL.xlsx")  
wb_5930_estadual = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_ESTADUAL.xlsx")  
wb_5930_municipal = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_MUNICIPAL.xlsx")  

ws_5930_nacional = wb_5930_nacional.active
ws_5930_estadual = wb_5930_estadual.active
ws_5930_municipal = wb_5930_municipal.active

lista_ws2 = [ws_5930_nacional, ws_5930_estadual, ws_5930_municipal]
lista_wb2 = [wb_5930_nacional, wb_5930_estadual, wb_5930_municipal]
for ws2, wb2 in zip(lista_ws2, lista_wb2):
    ajustar_colunas(ws2)
    ajustar_bordas(wb2)

# Salvar a planilha
wb_5930_nacional.save('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_NACIONAL.xlsx')
wb_5930_estadual.save('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_ESTADUAL.xlsx')
wb_5930_municipal.save('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_MUNICIPAL.xlsx')

#CARREGANDO AS PLANILHAS PEVS 289 E FAZENDO ALTERAÇÕIES NELAS
wb_289_nacional = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_289_NACIONAL.xlsx")  
wb_289_estadual = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_289_ESTADUAL.xlsx")  
 

ws_289_nacional = wb_289_nacional.active
ws_289_estadual = wb_289_estadual.active

lista_ws3 = [ws_289_nacional, ws_289_estadual]
lista_wb3 = [wb_289_nacional, wb_289_nacional]
for ws, wb in zip(lista_ws3, lista_wb3):
    ajustar_colunas(ws)
    ajustar_bordas(wb)
    
wb_289_nacional.save('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_289_NACIONAL.xlsx')
wb_289_estadual.save('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_289_ESTADUAL.xlsx')


#TESTANDO JUNTAR TODAS PLANILHAS EM UMA SÓ
planilha_principal = openpyxl.Workbook()

wb_291_nacional = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_NACIONAL.xlsx")  
wb_291_estadual = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_ESTADUAL.xlsx")  
wb_291_municipal = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_291_MUNICIPAL.xlsx")  
wb_5930_nacional = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_NACIONAL.xlsx")  
wb_5930_estadual = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_ESTADUAL.xlsx")  
wb_5930_municipal = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_5930_6549_MUNICIPAL.xlsx")  
wb_289_nacional = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_289_NACIONAL.xlsx")  
wb_289_estadual = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS_289_ESTADUAL.xlsx")  

aba_291_nacional = planilha_principal.create_sheet("PEVS 291 NACIONAL")
aba_291_estadual = planilha_principal.create_sheet("PEVS 291 ESTADUAL")
aba_291_municipal = planilha_principal.create_sheet("PEVS 291 MUNICIPAL")
aba_5930_nacional = planilha_principal.create_sheet("PEVS 5930 NACIONAL")
aba_5930_estadual = planilha_principal.create_sheet("PEVS 5930 ESTADUAL")
aba_5930_municipal = planilha_principal.create_sheet("PEVS 5930 MUNICIPAL")
aba_289_nacional =  planilha_principal.create_sheet("PEVS 289 NACIONAL")
aba_289_estadual = planilha_principal.create_sheet("PEVS 289 ESTADUAL")

# Copiar os dados da primeira planilha para a nova planilha
for linha in wb_291_nacional.active.iter_rows(values_only=True):
    aba_291_nacional.append(linha)

for linha in wb_291_estadual.active.iter_rows(values_only=True):
    aba_291_estadual.append(linha)

for linha in wb_291_municipal.active.iter_rows(values_only=True):
    aba_291_municipal.append(linha)
    
for linha in wb_5930_nacional.active.iter_rows(values_only=True):
    aba_5930_nacional.append(linha)

for linha in wb_5930_estadual.active.iter_rows(values_only=True):
    aba_5930_estadual.append(linha)

for linha in wb_5930_municipal.active.iter_rows(values_only=True):
    aba_5930_municipal.append(linha)
    
for linha in wb_289_nacional.active.iter_rows(values_only=True):
    aba_289_nacional.append(linha)

for linha in wb_289_estadual.active.iter_rows(values_only=True):
    aba_289_estadual.append(linha)

for aba in planilha_principal.sheetnames:
    if aba not in ["PEVS 291 NACIONAL", "PEVS 291 ESTADUAL", "PEVS 291 MUNICIPAL", "PEVS 5930 NACIONAL","PEVS 5930 ESTADUAL", "PEVS 5930 MUNICIPAL", "PEVS 289 NACIONAL", "PEVS 289 ESTADUAL"]:
        del planilha_principal[aba]

lista_abas = [aba_291_nacional, aba_291_estadual, aba_291_municipal, aba_5930_nacional, aba_5930_estadual, aba_5930_municipal, aba_289_nacional, aba_289_estadual]
for abas in lista_abas:
    ajustar_colunas(abas)
    ajustar_colunas(abas)
    
planilha_principal.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS.xlsx")

worksheet = planilha_principal.active
df = pd.read_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\ANP\\ETANOL\\ETANOL ANP.xlsx')

for sheet_name in planilha_principal.sheetnames:
    worksheet = planilha_principal[sheet_name]
    
    for col_num in range(1, worksheet.max_column + 1):
        cell = worksheet.cell(row=1, column=col_num)
        cell.font = Font(bold=True)
        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

planilha_principal.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PEVS.xlsx")

#Faz autenticação do google drive para jogar os arquivos gerados
CLIENT_SECRET_FILE = 'credencials.json'
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ["https://www.googleapis.com/auth/drive"]

service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

#PASSA O PAM PARA O DRIVE
file_id = "19gtgCVHipi5E_-g4K__BaDMwzFvEb1g3"
FILE_NAMES = ["PEVS_291_NACIONAL.xlsx", "PEVS_291_ESTADUAL.xlsx", "PEVS_291_MUNICIPAL.xlsx", "PEVS_5930_6549_NACIONAL.xlsx", "PEVS_5930_6549_ESTADUAL.xlsx", "PEVS_5930_6549_MUNICIPAL.xlsx", "PEVS_289_NACIONAL.xlsx", "PEVS_289_ESTADUAL.xlsx"]
MIME_TYPES = ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", \
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", \
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]

#LISTA TODOS OS ARQUIVO DENTRO DA PASTA
def listar_arquivos():
    results = service.files().list(
        q=f"trashed=false and '{file_id}' in parents",
        spaces='drive',
        pageSize=10,
        fields="nextPageToken, files(id, name, createdTime)"
    ).execute()
    items = results.get('files', [])
    items_sorted = sorted(items, key=lambda x: x['createdTime']) 
    return items_sorted

def obter_id_do_arquivo(file_name):
    items = listar_arquivos()
    for item in items:
        if item['name'] == file_name:
            return item['id']
    return None 
    

#ADICIONA TODOS OS ARQUIVOS NA PASTA
for file_name, mime_type in zip(FILE_NAMES, MIME_TYPES):
    id_arquivo = obter_id_do_arquivo(file_name)

    if id_arquivo:
    
        media_replace = MediaFileUpload("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\{0}".format(file_name), mimetype=mime_type)
        service.files().update(
            fileId=id_arquivo,
            media_body=media_replace
        ).execute()
        print(f"Documento '{file_name}' atualizado")
    else:
        file_metadata = {
            "name": file_name,
            "parents": [file_id]
        }
        media = MediaFileUpload("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\{0}".format(file_name), mimetype=mime_type)

        service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()
        print(f"Arquivo '{file_name}' criado")
 



'''
#ABRE O ARQUIVO SQL.PY E EXECUTA TODOS OS COMANDOS DENTRO DELE
if __name__ == '__main__':
    from sql import executar_sql  # Importe a função aqui para evitar o erro de importação cíclica
    executar_sql()
'''
