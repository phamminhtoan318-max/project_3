import sys
from pathlib import Path
import logging
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config
from src.logger import setup_logging

logger = logging.getLogger(__name__)
file_path = config.RAW_DIR
staging_path = config.STAGING_DIR


def extract_data(file_name="amazon.csv", source_dir=file_path, target_dir=staging_path):
    input_path = Path(file_name) if Path(file_name).is_absolute() else source_dir / file_name

    if not input_path.exists():
        error_msg = f"Không tìm thấy file dữ liệu nguồn tại: {input_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    logger.info(f"Bắt đầu trích xuất dữ liệu từ: {input_path}")

    try:
        df = pd.read_csv(input_path)
        logger.info(f"Đọc thành công {len(df):,} dòng và {len(df.columns)} cột từ {input_path.name}.")

        target_dir.mkdir(parents=True, exist_ok=True)

        output_file_name = f"{input_path.stem}.parquet"
        output_path = target_dir / output_file_name

        logger.info(f"Đang lưu dữ liệu vào staging: {output_path}...")
        df.to_parquet(output_path, engine="pyarrow", index=False)

        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(
            f"Trích xuất và nạp vào staging hoàn tất: {output_path.name} "
            f"(Kích thước: {file_size_mb:.2f} MB, Tổng số dòng: {len(df):,})"
        )

        return df

    except pd.errors.EmptyDataError:
        logger.error(f"File nguồn {input_path} rỗng, không có dữ liệu để đọc.")
        raise
    except Exception as e:
        logger.exception(f"Lỗi xảy ra trong quá trình trích xuất dữ liệu từ {input_path}: {e}")
        raise


# if __name__ == "__main__":
#     setup_logging(log_filename="etl_process.log")
#     logger.info("Chạy trực tiếp module extract.py...")
#     extract_data()
