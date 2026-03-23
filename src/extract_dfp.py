import pandas as pd
import requests
import zipfile
import io
import os
import time
from src.config import DATA_RAW_DFP
from src.logger import setup_logger

logger = setup_logger()

def run_extract_dfp():

    start_time = time.time()

    empresas = [
                '001210',
                '001023',
                '019348',
                '000906',
                '020532',
                '025917',
                '024295',
                '018465',
                '026620',
                '020257',
                '002437',
                '017329',
                '018660',
                '020605',
                '014460',
                '020915',
                '020770',
                '018627',
                '014443',
                '019445',
                '023159',
                '023795',
                '016659',
                '024180',
                '019992',
                '024910',
                '020362',
                '022470',
                '008133',
                '006505',
                '004170',
                '003980',
                '025585',
                '020575',
                '020788',
                '016292'
                ]

    for j in empresas:

        link = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2025.zip'
        r = requests.get(link)

        zf = zipfile.ZipFile(io.BytesIO(r.content))
        arquivo = 'dfp_cia_aberta_DRE_con_2025.csv'

        dados = zf.open(arquivo)
        linhas = dados.readlines()

        lines = [i.strip().decode('ISO-8859-1') for i in linhas]
        lines = [i.split(';') for i in lines]

        df = pd.DataFrame(lines[1:], columns=lines[0])
        df['VL_AJUSTADO'] = pd.to_numeric(df['VL_CONTA'])

        filtro = df[df['CD_CVM'] == j]

        caminho = os.path.join(DATA_RAW_DFP, f'DFP_{j}.xlsx')

        with pd.ExcelWriter(caminho, engine='xlsxwriter') as writer:
            filtro.to_excel(writer, sheet_name='DRE')

        logger.info(f"DFP salvo: {caminho}")

    logger.info(f"Tempo total DFP: {time.time() - start_time}")