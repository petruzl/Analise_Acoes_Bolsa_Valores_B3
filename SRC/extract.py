import pandas as pd
import os
import logging

def carregar_dados(caminho):
    bpp = pd.DataFrame()
    dre = pd.DataFrame()

    arquivos = [f for f in os.listdir(caminho) if f.endswith('.xlsx')]

    for arq in arquivos:
        try:
            df_bpp = pd.read_excel(os.path.join(caminho, arq), sheet_name='BPP')
            df_dre = pd.read_excel(os.path.join(caminho, arq), sheet_name='DRE')

            if not df_bpp.empty:
                bpp = pd.concat([bpp, df_bpp])

            if not df_dre.empty:
                dre = pd.concat([dre, df_dre])

            logging.info(f"Arquivo carregado: {arq}")

        except Exception as e:
            logging.error(f"Erro no arquivo {arq}: {e}")

    return bpp, dre