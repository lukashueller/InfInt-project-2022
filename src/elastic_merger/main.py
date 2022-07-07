import logging
import os

from join_companies import CompanyJoiner

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)



if __name__ == "__main__":
    CompanyJoiner().join_rb_announcements()
