from src.extract_itr import run_extract_itr
from src.extract_dfp import run_extract_dfp
from src.transform import run_transform
from src.logger import setup_logger

logger = setup_logger()

def main():

    logger.info("INÍCIO DO PIPELINE")

    # ETL
    run_extract_itr()
    run_extract_dfp()
    run_transform() 

    logger.info("Pipeline finalizado")

if __name__ == "__main__":
    main()