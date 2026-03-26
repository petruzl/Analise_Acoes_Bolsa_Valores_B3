import pandas as pd
import requests
import zipfile
import io
import os
import time
from src.config import DATA_RAW_ITR
from src.logger import setup_logger

logger = setup_logger()

def run_extract_itr():

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

    demonstrativos = ['BPA', 'BPP', 'DRE']

    for j in empresas:
        lista_df = []

        for k in demonstrativos:
            link = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_2025.zip'
            r = requests.get(link)

            zf = zipfile.ZipFile(io.BytesIO(r.content))
            arquivo = f'itr_cia_aberta_{k}_con_2025.csv'

            dados = zf.open(arquivo)
            linhas = dados.readlines()

            lines = [i.strip().decode('ISO-8859-1') for i in linhas]
            lines = [i.split(';') for i in lines]

            df = pd.DataFrame(lines[1:], columns=lines[0])
            df['VL_AJUSTADO'] = pd.to_numeric(df['VL_CONTA'])

            filtro = df[df['CD_CVM'] == str(j).zfill(6)]
            lista_df.append(filtro)

            logger.info(f"{j} - {k} carregado {filtro.shape}")

        caminho = os.path.join(DATA_RAW_ITR, f'ITR_{j}.xlsx')

        with pd.ExcelWriter(caminho, engine='xlsxwriter') as writer:
            lista_df[0].to_excel(writer, sheet_name='BPA')
            lista_df[1].to_excel(writer, sheet_name='BPP')
            lista_df[2].to_excel(writer, sheet_name='DRE')

        logger.info(f"Arquivo salvo: {caminho}")

    logger.info(f"Tempo total ITR: {time.time() - start_time}")