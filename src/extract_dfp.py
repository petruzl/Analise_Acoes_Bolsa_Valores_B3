import pandas as pd
import requests
import zipfile
import io
import os
import time
from src.config import DATA_RAW_DFP
from src.logger import setup_logger

logger = setup_logger()

def baixar_com_retry(url, tentativas=3):
    for i in range(tentativas):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r
        except Exception as e:
            logger.warning(f"Tentativa {i+1} falhou: {e}")
            time.sleep(2)

    raise Exception(f"Falha ao baixar após {tentativas} tentativas")

def run_extract_dfp():

    start_time = time.time()

    empresas = [
                '020605'
                ]
    try:
        # 🔥 BAIXA UMA VEZ SÓ
        link = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2025.zip'
        r = baixar_com_retry(link)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        arquivo = 'dfp_cia_aberta_DRE_con_2025.csv'
        dados = zf.open(arquivo)
        linhas = dados.readlines()
        lines = [i.strip().decode('ISO-8859-1') for i in linhas]
        lines = [i.split(';') for i in lines]
        df = pd.DataFrame(lines[1:], columns=lines[0])
        df['VL_AJUSTADO'] = pd.to_numeric(df['VL_CONTA'])
    except Exception as e:
        logger.error(f"Erro ao baixar/processar DFP: {e}")
        return  # 🔥 para tudo se nem conseguiu baixar

    # 🔹 LOOP SÓ FILTRANDO
    for j in empresas:
        try:
            filtro = df[df['CD_CVM'] == j]

            caminho = os.path.join(DATA_RAW_DFP, f'DFP_{j}.xlsx')

            with pd.ExcelWriter(caminho, engine='xlsxwriter') as writer:
                filtro.to_excel(writer, sheet_name='DRE')

            logger.info(f"DFP salvo: {caminho}")

            time.sleep(0.5)  # leve pausa

        except Exception as e:
            logger.error(f"Erro na empresa {j}: {e}")

    logger.info(f"Tempo total DFP: {time.time() - start_time}")