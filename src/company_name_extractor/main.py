import logging
import os

from company_name_extractor import CompanyNameExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)



if __name__ == "__main__":
    CompanyNameExtractor().read_dump()
