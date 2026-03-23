import pandas as pd
import logging
import os
from src.extract import carregar_dados
from src.transform import calcular_indicadores
from src.market import buscar_dados_mercado
from src.load import salvar_resultado

# LOGGING PROFISSIONAL
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    logging.info("🚀 Iniciando pipeline de análise LPA/VPA")

    caminho = "data/raw"

    # 1. EXTRAÇÃO
    bpp, dre = carregar_dados(caminho)

    # 2. TRANSFORMAÇÃO
    indicadores = calcular_indicadores(bpp, dre)

    # 3. MERCADO
    mercado = buscar_dados_mercado()

    # 4. JOIN FINAL
    df_final = indicadores.merge(mercado, left_index=True, right_on="Empresa", how="left")

    # 5. LOAD
    salvar_resultado(df_final)

    logging.info("✅ Pipeline finalizado com sucesso")

if __name__ == "__main__":
    main()