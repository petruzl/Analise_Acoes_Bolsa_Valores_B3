import pandas as pd
import os
import numpy as np
from src.config import DATA_RAW_ITR, DATA_OUTPUT
from src.logger import setup_logger
import yfinance as yf

logger = setup_logger()

def run_transform():

    logger.info("Iniciando transformação e cálculo de indicadores")

    arquivos = [os.path.join(DATA_RAW_ITR, f) for f in os.listdir(DATA_RAW_ITR) if f.endswith('.xlsx')]

    bpp_consolidado = pd.DataFrame()
    dre_consolidado = pd.DataFrame()

    # 🔹 Leitura dos arquivos
    for f in arquivos:
        try:
            bpp_consolidado = pd.concat([bpp_consolidado, pd.read_excel(f, sheet_name='BPP')])
            dre_consolidado = pd.concat([dre_consolidado, pd.read_excel(f, sheet_name='DRE')])
            logger.info(f"Arquivo carregado: {os.path.basename(f)}")
        except Exception as e:
            logger.error(f"Erro ao ler {f}: {e}")

    # 🔹 Pivot
    bpp_pivot = pd.pivot_table(
        bpp_consolidado,
        index=['DENOM_CIA', 'DS_CONTA'],
        columns=['DT_FIM_EXERC'],
        values='VL_AJUSTADO'
    )

    dre_pivot = pd.pivot_table(
        dre_consolidado,
        index=['DENOM_CIA', 'DS_CONTA'],
        columns=['DT_FIM_EXERC'],
        values='VL_AJUSTADO'
    )

    lucro_liq_TTM = pd.DataFrame()
    patrimonio_liq = pd.DataFrame()

    empresas = bpp_pivot.index.get_level_values('DENOM_CIA').unique()

    # 🔹 Cálculos contábeis
    for emp in empresas:
        try:
            ll = dre_pivot.loc[(emp, 'Lucro/Prejuízo Consolidado do Período')].dropna()
            if len(ll) >= 4:
                lucro_liq_TTM.loc[emp, 'Lucro_TTM'] = ll.tail(4).sum()
        except:
            pass

        contas_pl = [
            'Patrimônio Líquido Consolidado',
            'Patrimônio Líquido',
            'Patrimônio Líquido Atribuído à Controladora'
        ]

        for c in contas_pl:
            try:
                if (emp, c) in bpp_pivot.index:
                    patrimonio_liq.loc[emp, 'PL_Ajustado'] = bpp_pivot.loc[(emp, c)].dropna().iloc[-1]
                    break
            except:
                continue

    # 🔹 Mapeamento ticker -> empresa (ESSENCIAL)
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

    tickers = list(mapa_empresas.keys())

    resultados = pd.DataFrame()

    # 🔹 Dados de mercado + indicadores
    for t in tickers:
        try:
            obj = yf.Ticker(t)

            preco = obj.history(period='1d')['Close'].iloc[-1]
            acoes = obj.info.get('sharesOutstanding', np.nan)

            empresa_nome = mapa_empresas[t]

            lucro = lucro_liq_TTM.loc[empresa_nome, 'Lucro_TTM'] if empresa_nome in lucro_liq_TTM.index else np.nan
            pl = patrimonio_liq.loc[empresa_nome, 'PL_Ajustado'] if empresa_nome in patrimonio_liq.index else np.nan

            # 🔹 Indicadores
            lpa = lucro / acoes if acoes and not pd.isna(lucro) else np.nan
            vpa = pl / acoes if acoes and not pd.isna(pl) else np.nan

            p_l = preco / lpa if lpa and not pd.isna(lpa) else np.nan
            p_vpa = preco / vpa if vpa and not pd.isna(vpa) else np.nan

            # 🔹 Resultado final
            resultados.loc[t, 'Empresa'] = empresa_nome
            resultados.loc[t, 'Preco'] = preco
            resultados.loc[t, 'LPA_TTM'] = lpa
            resultados.loc[t, 'VPA'] = vpa
            resultados.loc[t, 'P_L'] = p_l
            resultados.loc[t, 'P_VPA'] = p_vpa
            resultados.loc[t, 'N_Acoes_Yahoo'] = acoes

            logger.info(f"{t} processado com sucesso")

        except Exception as e:
            logger.error(f"Erro mercado {t}: {e}")

    # 🔹 Ordenar colunas igual seu print
    resultados = resultados[
        ['Empresa', 'Preco', 'LPA_TTM', 'VPA', 'P_L', 'P_VPA', 'N_Acoes_Yahoo']
    ]

    # 🔹 Salvar
    caminho_saida = os.path.join(DATA_OUTPUT, "resultado.xlsx")
    resultados.to_excel(caminho_saida)

    logger.info(f"Resultado salvo em {caminho_saida}")