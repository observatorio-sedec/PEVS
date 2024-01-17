import sqlite3
from principal import df291nacional, df291estadual, df291municipal, df6549nacional, df6549estadual, df6549municipal


def executar_sql():
    con = sqlite3.connect('Z:\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\DATAHUB_DATABASE.db')
    cur = con.cursor()

    
    # Verifica a existência das tabelas e retorna 1
    verificando_existencia_291_nacional = '''
    SELECT 1
    FROM sqlite_master
    WHERE type='table' AND name='pevs_291_nacional';
    '''
    verificando_existencia_291_estadual = '''
    SELECT 1
    FROM sqlite_master
    WHERE type='table' AND name='pevs_291_estadual';
    '''
    verificando_existencia_291_municipal = '''
    SELECT 1
    FROM sqlite_master
    WHERE type='table' AND name='pevs_291_municipal';
    '''

    verificando_existencia_5930_nacional = '''
    SELECT 1
    FROM sqlite_master
    WHERE type='table' AND name='pevs_5930_6549_nacional';
    '''
    verificando_existencia_5930_estadual = '''
    SELECT 1
    FROM sqlite_master
    WHERE type='table' AND name='pevs_5930_6549_estadual';
    '''
    verificando_existencia_5930_municipal = '''
    SELECT 1
    FROM sqlite_master
    WHERE type='table' AND name='pevs_5930_6549_municipal';
    '''

    # Execute as consultas de verificação
    cur.execute(verificando_existencia_291_nacional)
    resultado_291_nacional = cur.fetchone()
    cur.execute(verificando_existencia_291_estadual)
    resultado_291_estadual = cur.fetchone()
    cur.execute(verificando_existencia_291_municipal)
    resultado_291_municipal = cur.fetchone()

    cur.execute(verificando_existencia_5930_nacional)
    resultado_5930_nacional = cur.fetchone()
    cur.execute(verificando_existencia_5930_estadual)
    resultado_5930_estadual = cur.fetchone()
    cur.execute(verificando_existencia_5930_municipal)
    resultado_5930_municipal = cur.fetchone()

    # Verifique se as tabelas existem e exclua, se necessário
    #VERIFICAÇÃO PEVS 291
    if resultado_291_nacional and resultado_291_nacional[0] == 1:
        dropando_tabela_291_nacional = '''
        DROP TABLE pevs_291_nacional;
        '''
        cur.execute(dropando_tabela_291_nacional)

    if resultado_291_estadual and resultado_291_estadual[0] == 1:
        dropando_tabela_291_estadual = '''
        DROP TABLE pevs_291_estadual;
        '''
        cur.execute(dropando_tabela_291_estadual)

    if resultado_291_municipal and resultado_291_municipal[0] == 1:
        dropando_tabela_291_municipal = '''
        DROP TABLE pevs_291_municipal;
        '''
        cur.execute(dropando_tabela_291_municipal)

    #VERIFICAÇÃO PEVS 5930
    if resultado_5930_nacional and resultado_5930_nacional[0] == 1:
        dropando_tabela_5930_nacional = '''
        DROP TABLE pevs_5930_6549_nacional;
        '''
        cur.execute(dropando_tabela_5930_nacional)

    if resultado_5930_estadual and resultado_5930_estadual[0] == 1:
        dropando_tabela_5930_estadual = '''
        DROP TABLE pevs_5930_6549_estadual;
        '''
        cur.execute(dropando_tabela_5930_estadual)

    if resultado_5930_municipal and resultado_5930_municipal[0] == 1:
        dropando_tabela_5930_municipal = '''
        DROP TABLE pevs_5930_6549_municipal;
        '''
        cur.execute(dropando_tabela_5930_municipal)

    pevs_291_nacional = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_291_nacional (
        id_pevs_291_nacional INTEGER PRIMARY KEY AUTOINCREMENT,
        id INTEGER NOT NULL,
        nome TEXT,
        produto TEXT,
        quantidade_producao NUMERIC,
        unidade_quantidade TEXT, 
        valor_producao NUMERIC,
        unidade_producao TEXT,
        ano DATE); 
    '''
    pevs_291_estadual = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_291_estadual (
        id_pevs_291_estadual INTEGER PRIMARY KEY AUTOINCREMENT,
        id INTEGER NOT NULL,
        nome TEXT,
        produto TEXT,
        quantidade_producao NUMERIC,
        unidade_quantidade TEXT, 
        valor_producao NUMERIC,
        unidade_producao TEXT,
        ano DATE); 
    '''
    pevs_291_municipal = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_291_municipal (
        id_pevs_291_municipal INTEGER PRIMARY KEY AUTOINCREMENT,
        id INTEGER NOT NULL,
        nome TEXT,
        produto TEXT,
        quantidade_producao NUMERIC,
        unidade_quantidade TEXT, 
        valor_producao NUMERIC,
        unidade_producao TEXT,
        ano DATE); 
    '''

    pevs_5930_6549_nacional = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_5930_6549_nacional (
        id_pevs_5930_6549_nacional INTEGER PRIMARY KEY AUTOINCREMENT,
        id INTEGER NOT NULL,
        nome TEXT,
        especie TEXT,
        area_total NUMERIC,
        unidade TEXT, 
        ano DATE); 
    '''
    pevs_5930_6549_estadual = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_5930_6549_estadual (
        id_pevs_5930_6549_estadual INTEGER PRIMARY KEY AUTOINCREMENT,
        id INTEGER NOT NULL,
        nome TEXT,
        especie TEXT,
        area_total NUMERIC,
        unidade TEXT, 
        ano DATE); 
    '''
    pevs_5930_6549_municipal = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_5930_6549_municipal (
        id_pevs_5930_6549_municipal INTEGER PRIMARY KEY AUTOINCREMENT,
        id INTEGER NOT NULL,
        nome TEXT,
        especie TEXT,
        area_total NUMERIC,
        unidade TEXT, 
        ano DATE); 
    '''

    cur.execute(pevs_291_nacional)
    cur.execute(pevs_291_estadual)
    cur.execute(pevs_291_municipal)
    cur.execute(pevs_5930_6549_nacional)
    cur.execute(pevs_5930_6549_estadual)
    cur.execute(pevs_5930_6549_municipal)

    #INSERINDO DADOS
    inserindo_pevs_291_nacional = \
    '''
    INSERT INTO pevs_291_nacional (id, nome, produto, quantidade_producao, unidade_quantidade, valor_producao, unidade_producao, ano)
    VALUES(?,?,?,?,?,?,?,?) 
    '''
    for idx, i in df291nacional.iterrows():
        dados = (
            i['id'], 
            i['nome'], 
            i['produto'], 
            i['Quantidade produzida na silvicultura'], 
            i['unidade_quantidade'], 
            i['Valor da produção na silvicultura'], 
            i['unidade_producao'],
            i['ano']
        )
        cur.execute(inserindo_pevs_291_nacional, dados)

    inserindo_pevs_291_estadual = \
    '''
    INSERT INTO pevs_291_estadual (id, nome, produto, quantidade_producao, unidade_quantidade, valor_producao, unidade_producao, ano)
    VALUES(?,?,?,?,?,?,?,?) 
    '''
    for idx, i in df291estadual.iterrows():
        dados = (
            i['id'], 
            i['nome'], 
            i['produto'], 
            i['Quantidade produzida na silvicultura'], 
            i['unidade_quantidade'], 
            i['Valor da produção na silvicultura'], 
            i['unidade_producao'],
            i['ano']
        )
        cur.execute(inserindo_pevs_291_estadual, dados)

    inserindo_pevs_291_municipal = \
    '''
    INSERT INTO pevs_291_municipal (id, nome, produto, quantidade_producao, unidade_quantidade, valor_producao, unidade_producao, ano)
    VALUES(?,?,?,?,?,?,?,?) 
    '''
    for idx, i in df291municipal.iterrows():
        dados = (
            i['id'], 
            i['nome'], 
            i['produto'], 
            i['Quantidade produzida na silvicultura'], 
            i['unidade_quantidade'], 
            i['Valor da produção na silvicultura'], 
            i['unidade_producao'],
            i['ano']
        )
        cur.execute(inserindo_pevs_291_municipal, dados)

    inserindo_pevs_5930_6549_nacional = \
    '''
    INSERT INTO pevs_5930_6549_nacional (id, nome, especie, area_total, unidade, ano)
    VALUES(?,?,?,?,?,?) 
    '''
    for idx, i in df6549nacional.iterrows():
        dados = (
            i['id'], 
            i['nome'], 
            i['especie'], 
            i['Área total existente em 31/12 dos efetivos da silvicultura'], 
            i['unidade'], 
            i['ano']
        )
        cur.execute(inserindo_pevs_5930_6549_nacional, dados)

    inserindo_pevs_5930_6549_estadual = \
    '''
    INSERT INTO pevs_5930_6549_estadual (id, nome, especie, area_total, unidade, ano)
    VALUES(?,?,?,?,?,?) 
    '''
    for idx, i in df6549estadual.iterrows():
        dados = (
            i['id'], 
            i['nome'], 
            i['especie'], 
            i['Área total existente em 31/12 dos efetivos da silvicultura'], 
            i['unidade'], 
            i['ano']
        )
        cur.execute(inserindo_pevs_5930_6549_estadual, dados)

    inserindo_pevs_5930_6549_municipal = \
    '''
    INSERT INTO pevs_5930_6549_municipal (id, nome, especie, area_total, unidade, ano)
    VALUES(?,?,?,?,?,?) 
    '''
    for idx, i in df6549municipal.iterrows():
        dados = (
            i['id'], 
            i['nome'], 
            i['especie'], 
            i['Área total existente em 31/12 dos efetivos da silvicultura'], 
            i['unidade'], 
            i['ano']
        )
        cur.execute(inserindo_pevs_5930_6549_municipal, dados)

    con.commit()
    con.close()