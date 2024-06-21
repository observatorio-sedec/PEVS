import psycopg2
from principal import df291estadual, df291municipal, df6549estadual, df6549municipal, df289estadual
from conexão import conexao

def executar_sql():
    cur = conexao.cursor()
    
    # Verifica a existência das tabelas e retorna 1

    verificando_existencia_291_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_type='BASE TABLE' AND table_name='pevs_291_estadual';
    '''
    
    verificando_existencia_291_municipal = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_type='BASE TABLE' AND table_name='pevs_291_municipal';
    '''
    
    verificando_existencia_5930_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_type='BASE TABLE' AND table_name='pevs_5930_6549_estadual';
    '''
    
    verificando_existencia_5930_municipal = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_type='BASE TABLE' AND table_name='pevs_5930_6549_municipal';
    '''
    
    verificando_existencia_289_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_type='BASE TABLE' AND table_name='pevs_289_estadual';
    '''
    pevs_291_estadual = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_291_estadual (
        id_pevs_291_estadual SERIAL PRIMARY KEY,
        id INTEGER NOT NULL,
        nome TEXT,
        produto TEXT,
        quantidade_producao INTEGER,
        unidade_quantidade TEXT, 
        valor_producao INTEGER,
        unidade_producao TEXT,
        ano DATE); 
    '''
    pevs_291_municipal = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_291_municipal (
        id_pevs_291_municipal SERIAL PRIMARY KEY ,
        id INTEGER NOT NULL,
        nome TEXT,
        produto TEXT,
        quantidade_producao INTEGER,
        unidade_quantidade TEXT, 
        valor_producao INTEGER,
        unidade_producao TEXT,
        ano DATE); 
    '''
    pevs_5930_6549_estadual = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_5930_6549_estadual (
        id_pevs_5930_6549_estadual SERIAL PRIMARY KEY ,
        id INTEGER NOT NULL,
        nome TEXT,
        especie TEXT,
        area_total INTEGER,
        unidade TEXT, 
        ano DATE); 
    '''
    pevs_5930_6549_municipal = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_5930_6549_municipal (
        id_pevs_5930_6549_municipal SERIAL PRIMARY KEY,
        id INTEGER NOT NULL,
        nome TEXT,
        especie TEXT,
        area_total INTEGER,
        unidade TEXT, 
        ano DATE); 
    '''
    
    pevs_289_estadual = \
    '''
    CREATE TABLE IF NOT EXISTS pevs_289_estadual (
        id_pevs_289_estadual SERIAL PRIMARY KEY ,
        id INTEGER NOT NULL,
        nome TEXT,
        produto TEXT,
        quantidade_producao INTEGER,
        unidade_quantidade TEXT, 
        valor_producao INTEGER,
        unidade_producao TEXT,
        ano DATE); 
    '''

    cur.execute(pevs_291_estadual)
    cur.execute(pevs_291_municipal)
    cur.execute(pevs_5930_6549_estadual)
    cur.execute(pevs_5930_6549_municipal)
    cur.execute(pevs_289_estadual)

    # Execute as consultas de verificação
    cur.execute(verificando_existencia_291_estadual)
    resultado_291_estadual = cur.fetchone()
    cur.execute(verificando_existencia_291_municipal)
    resultado_291_municipal = cur.fetchone()


    cur.execute(verificando_existencia_5930_estadual)
    resultado_5930_estadual = cur.fetchone()
    cur.execute(verificando_existencia_5930_municipal)
    resultado_5930_municipal = cur.fetchone()
    cur.execute(verificando_existencia_289_estadual)
    resultado_289_estadual = cur.fetchone()

    # Verifique se as tabelas existem e exclua, se necessário
    if resultado_291_estadual[0] == 1:
        dropando_tabela_291_estadual = '''
        TRUNCATE TABLE pevs_291_estadual;
        '''
        cur.execute(dropando_tabela_291_estadual)
    else:
        pass

    if resultado_291_municipal[0] == 1:
        dropando_tabela_291_municipal = '''
        TRUNCATE TABLE pevs_291_municipal;
        '''
        cur.execute(dropando_tabela_291_municipal)
    else:
        pass

    if resultado_5930_estadual[0] == 1:
        dropando_tabela_5930_estadual = '''
        TRUNCATE TABLE pevs_5930_6549_estadual;
        '''
        cur.execute(dropando_tabela_5930_estadual)
    else:
        pass    
    
    if resultado_5930_municipal[0] == 1:
        dropando_tabela_5930_municipal = '''
        TRUNCATE TABLE pevs_5930_6549_municipal;
        '''
        cur.execute(dropando_tabela_5930_municipal)
    else:
        pass        
    if resultado_289_estadual[0] == 1:
        dropando_tabela_289_estadual = '''
        TRUNCATE TABLE pevs_289_estadual;
        '''
        cur.execute(dropando_tabela_289_estadual)
    else:
        pass

    #INSERINDO DADOS
    inserindo_pevs_291_estadual = \
    '''
    INSERT INTO pevs_291_estadual (id, nome, produto, quantidade_producao, unidade_quantidade, valor_producao, unidade_producao, ano)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
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
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")

    inserindo_pevs_291_municipal = \
    '''
    INSERT INTO pevs_291_municipal (id, nome, produto, quantidade_producao, unidade_quantidade, valor_producao, unidade_producao, ano)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
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
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")


    inserindo_pevs_5930_6549_estadual = \
    '''
    INSERT INTO pevs_5930_6549_estadual (id, nome, especie, area_total, unidade, ano)
    VALUES(%s,%s,%s,%s,%s,%s) 
    '''
    try:
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
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")

    inserindo_pevs_5930_6549_municipal = \
    '''
    INSERT INTO pevs_5930_6549_municipal (id, nome, especie, area_total, unidade, ano)
    VALUES(%s,%s,%s,%s,%s,%s) 
    '''
    try:
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
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")
        
    inserindo_pevs_289_estadual = \
    '''
    INSERT INTO pevs_289_estadual (id, nome, produto, quantidade_producao, unidade_quantidade, valor_producao, unidade_producao, ano)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df289estadual.iterrows():
            dados = (
                i['id'], 
                i['nome'], 
                i['produto'], 
                i['Quantidade produzida na extração vegetal'], 
                i['unidade_quantidade'], 
                i['Valor da produção na extração vegetal'], 
                i['unidade_producao'],
                i['ano']
            )
            cur.execute(inserindo_pevs_289_estadual, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")

    conexao.commit()
    conexao.close()