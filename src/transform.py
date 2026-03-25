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
                        'JHSF3.SA': 'JHSF PARTICIPACOES S.A.'
                        }
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

    lucro_liq_TTM = pd.DataFrame()
    patrimonio_liq = pd.DataFrame()

    empresas = bpp_pivot.index.get_level_values('DENOM_CIA').unique()

    # 🔹 Cálculo indicadores fundamentalistas
    for emp in empresas:

        # Lucro TTM
        try:
            ll = dre_pivot.loc[(emp, 'Lucro/Prejuízo Consolidado do Período')].dropna()
            if len(ll) >= 4:
                lucro_liq_TTM.loc[emp, 'Lucro_TTM'] = ll.tail(4).sum()
        except:
            pass

        # Patrimônio Líquido
        contas_pl = [
            'Patrimônio Líquido Consolidado',
            'Patrimônio Líquido',
            'Patrimônio Líquido Atribuído à Controladora'
        ]

        for c in contas_pl:
            try:
                if (emp, c) in bpp_pivot.index:
                    pl = bpp_pivot.loc[(emp, c)].dropna()
                    if not pl.empty:
                        patrimonio_liq.loc[emp, 'PL_Ajustado'] = pl.iloc[-1]
                        break
            except:
                continue

    # 🔹 Mercado (Yahoo Finance)
    tickers = [
                'JHSF3.SA'
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

    # 🔹 Merge dados fundamentalistas
    df_final = resultados.merge(
        lucro_liq_TTM,
        left_on='Empresa',
        right_index=True,
        how='left'
    )

    df_final = df_final.merge(
        patrimonio_liq,
        left_on='Empresa',
        right_index=True,
        how='left'
    )

    # 🔹 Cálculo indicadores
    df_final['LPA'] = df_final['Lucro_TTM'] / df_final['Acoes']
    df_final['VPA'] = df_final['PL_Ajustado'] / df_final['Acoes']
    df_final['P_L'] = df_final['Preco'] / df_final['LPA']
    df_final['P_VPA'] = df_final['Preco'] / df_final['VPA']

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