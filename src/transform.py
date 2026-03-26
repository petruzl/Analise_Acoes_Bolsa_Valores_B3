import pandas as pd
import os
import numpy as np
from datetime import datetime
from src.config import DATA_RAW_ITR, DATA_OUTPUT
from src.logger import setup_logger
import yfinance as yf
from src.database import get_engine

logger = setup_logger()

mapa_empresas = {
                        'BRSR3.SA': 'BCO ESTADO DO RIO GRANDE DO SUL S.A.',
                        'BBAS3.SA': 'BCO BRASIL S.A.',
                        'ITUB3.SA': 'ITAU UNIBANCO HOLDING S.A.',
                        'BBDC3.SA': 'BCO BRADESCO S.A.',
                        'SANB3.SA': 'BCO SANTANDER (BRASIL) S.A.',
                        'RAIZ4.SA': 'RAÍZEN S.A.',
                        'VBBR3.SA': 'VIBRA ENERGIA S/A',
                        'UGPA3.SA': 'ULTRAPAR PARTICIPACOES S.A.',
                        'AURE3.SA': 'AUREN ENERGIA S.A.',
                        'TAEE3.SA': 'TRANSMISSORA ALIANÇA DE ENERGIA ELÉTRICA S.A.',
                        'AXIA3.SA': 'CENTRAIS ELET BRAS S.A. - ELETROBRAS',
                        'EGIE3.SA': 'ENGIE BRASIL ENERGIA S.A.',
                        'CPFE3.SA': 'CPFL ENERGIA S.A.',
                        'JHSF3.SA': 'JHSF PARTICIPACOES S.A.',
                        'CYRE3.SA': 'CYRELA BRAZIL REALTY S.A.EMPREEND E PART',
                        'MRVE3.SA': 'MRV ENGENHARIA E PARTICIPACOES S.A.',
                        'EZTC3.SA': 'EZ TEC EMPREEND. E PARTICIPACOES S.A.',
                        'SAPR3.SA': 'CIA. DE SANEAMENTO DO PARANÁ - SANEPAR',
                        'SBSP3.SA': 'CIA SANEAMENTO BASICO EST SAO PAULO',
                        'CSMG3.SA': 'CIA SANEAMENTO DE MINAS GERAIS-COPASA MG',
                        'BBSE3.SA': 'BB SEGURIDADE PARTICIPAÇÕES S.A.',
                        'CXSE3.SA': 'CAIXA SEGURIDADE PARTICIPAÇÕES S.A.',
                        'PSSA3.SA': 'PORTO SEGURO S.A.',
                        'IRBR3.SA': 'IRB - BRASIL RESSEGUROS S.A.',
                        'TOTS3.SA': 'TOTVS S.A.',
                        'LWSA3.SA': 'LWSA S/A',
                        'POSI3.SA': 'POSITIVO TECNOLOGIA S.A.',
                        'MGLU3.SA': 'MAGAZINE LUIZA S.A.',
                        'LREN3.SA': 'LOJAS RENNER S.A.',
                        'BHIA3.SA': 'GRUPO CASAS BAHIA S.A.',
                        'VALE3.SA': 'VALE S.A.',
                        'GGBR4.SA': 'GERDAU S.A.',
                        'CSNA3.SA': 'CSN MINERAÇÃO S.A.',
                        'JBSS3.SA': 'JBS S.A.',
                        'MRFG3.SA': 'MARFRIG GLOBAL FOODS S.A.',
                        'BRFS3.SA': 'BRF S.A.'
                        }
def normalizar(texto):
    if pd.isna(texto):
        return texto
    texto = str(texto).upper().strip()
    
    # remove pontuações problemáticas
    texto = texto.replace('.', '')
    texto = texto.replace('/', '')
    
    return texto
    

def run_transform():

    logger.info("Iniciando transformação e cálculo de indicadores")

    arquivos = [os.path.join(DATA_RAW_ITR, f) for f in os.listdir(DATA_RAW_ITR) if f.endswith('.xlsx')]

    bpp_consolidado = []
    dre_consolidado = []

    # 🔹 Leitura dos arquivos
    for f in arquivos:
        try:
            bpp = pd.read_excel(f, sheet_name='BPP')
            dre = pd.read_excel(f, sheet_name='DRE')

            bpp_consolidado.append(bpp)
            dre_consolidado.append(dre)

            logger.info(f"Arquivo carregado: {os.path.basename(f)}")

        except Exception as e:
            logger.error(f"Erro ao ler {f}: {e}")

    # 🔹 Concat (corrige warning)
    bpp_consolidado = [df for df in bpp_consolidado if not df.empty]
    dre_consolidado = [df for df in dre_consolidado if not df.empty]

    bpp_consolidado = pd.concat(bpp_consolidado, ignore_index=True)
    dre_consolidado = pd.concat(dre_consolidado, ignore_index=True)

    # 🔹 Pivot
    bpp_pivot = pd.pivot_table(
        bpp_consolidado,
        index=['DENOM_CIA', 'DS_CONTA'],
        columns='DT_FIM_EXERC',
        values='VL_AJUSTADO'
    )

    dre_pivot = pd.pivot_table(
        dre_consolidado,
        index=['DENOM_CIA', 'DS_CONTA'],
        columns='DT_FIM_EXERC',
        values='VL_AJUSTADO'
    )

    empresas = bpp_pivot.index.get_level_values('DENOM_CIA').unique()

    lucro_liq_TTM = pd.DataFrame()
    lucro_liq_TTM = lucro_liq_TTM * 1000

    patrimonio_liq = bpp_consolidado[bpp_consolidado['CD_CONTA'] == '2.07'].copy()

    # garantir ordenação por data
    patrimonio_liq = patrimonio_liq.sort_values('DT_FIM_EXERC')

    # pegar último valor por empresa
    patrimonio_liq = patrimonio_liq.groupby('DENOM_CIA').last()['VL_CONTA']

    print("\n--- EMPRESAS NO PATRIMONIO ---")
    print(patrimonio_liq.index.tolist())

    # 🔹 Cálculo indicadores fundamentalistas
    for emp in empresas:

        # Lucro TTM
        try:
            ll = dre_pivot.loc[(emp, 'Lucro/Prejuízo Consolidado do Período')].dropna()
            if len(ll) >= 4:
                lucro_liq_TTM.loc[emp, 'Lucro_TTM'] = ll.tail(4).sum()
        except:
            pass
    # 🔹 Mercado (Yahoo Finance)
    tickers = [
                'BRSR3.SA',
                'BBAS3.SA',
                'ITUB3.SA',
                'BBDC3.SA',
                'SANB3.SA',
                'RAIZ4.SA',
                'VBBR3.SA',
                'UGPA3.SA',
                'AURE3.SA',
                'TAEE3.SA',
                'AXIA3.SA',
                'EGIE3.SA',
                'CPFE3.SA',
                'JHSF3.SA',
                'CYRE3.SA',
                'MRVE3.SA',
                'EZTC3.SA',
                'SAPR3.SA',
                'SBSP3.SA',
                'CSMG3.SA',
                'BBSE3.SA',
                'CXSE3.SA',
                'PSSA3.SA',
                'IRBR3.SA',
                'TOTS3.SA',
                'LWSA3.SA',
                'POSI3.SA',
                'MGLU3.SA',
                'LREN3.SA',
                'BHIA3.SA',
                'VALE3.SA',
                'GGBR4.SA',
                'CSNA3.SA',
                'JBSS3.SA',
                'MRFG3.SA',
                'BRFS3.SA'
            ]

    resultados = pd.DataFrame()

    for t in tickers:
        try:
            obj = yf.Ticker(t)
            hist = obj.history(period='1d')

            if hist.empty:
                logger.warning(f"{t} sem dados")
                continue

            preco = hist['Close'].iloc[-1]
            acoes = obj.info.get('sharesOutstanding', np.nan)

            resultados.loc[t, 'Preco'] = preco
            resultados.loc[t, 'Acoes'] = acoes

            logger.info(f"{t} processado com sucesso")

        except Exception as e:
            logger.error(f"Erro mercado {t}: {e}")

    # 🔹 Ajuste nomes empresas (ticker → empresa)
    resultados['Empresa'] = resultados.index.to_series().map(mapa_empresas)

    # Corrige fillna (SEM inplace e usando Series)
    resultados['Empresa'] = resultados['Empresa'].fillna(resultados.index.to_series())
    # 🔥 NORMALIZAÇÃO (AQUI!)
    patrimonio_liq.index = patrimonio_liq.index.map(normalizar)
    lucro_liq_TTM.index = lucro_liq_TTM.index.map(normalizar)
    resultados['Empresa'] = resultados['Empresa'].map(normalizar)
    
    df_final = resultados.copy()

    patrimonio_liq.index = patrimonio_liq.index.map(normalizar)
    lucro_liq_TTM.index = lucro_liq_TTM.index.map(normalizar)
    resultados['Empresa'] = resultados['Empresa'].map(normalizar)

    print("\n--- INTERSEÇÃO ---")
    print(set(resultados['Empresa']) & set(patrimonio_liq.index))

    print("\n--- EMPRESAS CVM NORMALIZADAS ---")
    print(patrimonio_liq.index.tolist()[:20])

    print("\n--- EMPRESA YAHOO ---")
    print(resultados['Empresa'].tolist())

    print(any('JHSF' in x for x in patrimonio_liq.index))

    # 🔹 Merge dados fundamentalistas
    df_final = df_final.merge(
    patrimonio_liq.rename('PL_Ajustado'),
    left_on='Empresa',
    right_index=True,
    how='left'
    )  

    df_final = df_final.merge(
    lucro_liq_TTM,
    left_on='Empresa',
    right_index=True,
    how='left'
    )

    print(df_final[['Empresa', 'PL_Ajustado']].head())
    print("\n--- EMPRESAS RESULTADOS ---")
    print(resultados['Empresa'].unique())
    print(patrimonio_liq.loc['JHSF PARTICIPACOES SA'])
    # 🔹 Cálculo indicadores
    df_final['LPA'] = df_final['Lucro_TTM'] / df_final['Acoes']
    df_final['VPA'] = df_final['PL_Ajustado'] / df_final['Acoes']
    df_final['P_L'] = df_final['Preco'] / df_final['LPA']
    df_final['P_VPA'] = df_final['Preco'] / df_final['VPA']

    for idx, row in df_final.iterrows():
        logger.info(f"--- DEBUG {idx} ---")
        logger.info(f"Preço: {row['Preco']}")
        logger.info(f"PL: {row['PL_Ajustado']}")
        logger.info(f"Ações: {row['Acoes']}")
        logger.info(f"LPA: {row['LPA']}")
        logger.info(f"VPA: {row['VPA']}")
        logger.info(f"P/L: {row['P_L']}")
        logger.info(f"P/VPA: {row['P_VPA']}")
    
    print(patrimonio_liq.index.tolist())

    print(bpp_consolidado[
        bpp_consolidado['DS_CONTA'].str.contains('PATRIM', case=False, na=False)][['CD_CONTA', 'DS_CONTA', 'VL_CONTA']])

    # 🔹 Organização final (igual seu print)
    df_final = df_final[[
        'Empresa',
        'Preco',
        'LPA',
        'VPA',
        'P_L',
        'P_VPA',
        'Acoes'
    ]]

    df_final.rename(columns={
        'Acoes': 'N_Acoes_Yahoo'
    }, inplace=True)

    # 🔹 Salvar com nome dinâmico (evita erro PermissionError)
    nome_arquivo = f"resultado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    caminho_saida = os.path.join(DATA_OUTPUT, nome_arquivo)

    df_final.to_excel(caminho_saida, index=True)

    logger.info(f"Resultado salvo em {caminho_saida}")
    
    # 🔹 Salvar no SQL Server
    try:
        engine = get_engine()

        df_sql = df_final.copy()

        # reset index pra virar coluna
        df_sql.reset_index(inplace=True)
        df_sql.rename(columns={'index': 'Ticker'}, inplace=True)

        # adiciona data de execução (ESSENCIAL pra histórico)
        df_sql['Data_Execucao'] = datetime.now()

        df_sql.to_sql(
            'historico_acoes',  # nome da tabela
            con=engine,
            if_exists='append',  # 🔥 mantém histórico
            index=False
        )

        df_sql.drop_duplicates(subset=['Ticker', 'Data_Execucao'], inplace=True)

        logger.info("Dados salvos no SQL Server com sucesso")
    
    except Exception as e:
        logger.error(f"Erro ao salvar no SQL Server: {e}")