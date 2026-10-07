import sys
import logging
import urllib.parse
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text, types

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config
from src.logger import setup_logging

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Lớp xử lý việc lưu trữ và nạp dữ liệu (Load) vào Data Warehouse (SQL Server)
    và lưu bản sao Parquet vào tầng Processed.
    """

    # Ánh xạ kiểu dữ liệu tối ưu cho SQL Server (hỗ trợ Unicode tiếng Việt/ký tự lạ)
    SQL_DTYPE_MAP = {
        "product_id": types.NVARCHAR(50),
        "user_id": types.NVARCHAR(100),
        "review_id": types.NVARCHAR(50),
        "product_name": types.NVARCHAR(None),
        "category": types.NVARCHAR(500),
        "about_product": types.NVARCHAR(None),
        "user_name": types.NVARCHAR(None),
        "review_title": types.NVARCHAR(None),
        "review_content": types.NVARCHAR(None),
        "img_link": types.NVARCHAR(1000),
        "product_link": types.NVARCHAR(1000),
        "is_rating_missing": types.Boolean(),
    }

    def __init__(self, connection_string=None, processed_dir=None):
        self.connection_string = connection_string or config.DB_CONNECTION_STRING
        self.processed_dir = processed_dir or config.PROCESSED_DIR
        self._engine = None

    def get_engine(self):
        """Khởi tạo SQLAlchemy engine hỗ trợ fast_executemany cho tốc độ tải cao."""
        if self._engine is None:
            try:
                encoded_conn_str = urllib.parse.quote_plus(self.connection_string)
                self._engine = create_engine(
                    f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}",
                    fast_executemany=True,
                )
                logger.info("Khởi tạo SQLAlchemy engine kết nối SQL Server thành công.")
            except Exception as e:
                logger.exception(f"Lỗi khi khởi tạo SQLAlchemy engine: {e}")
                raise
        return self._engine

    def save_to_processed(self, df, filename="amazon_transformed.parquet"):
        """Lưu bản sao dữ liệu đã biến đổi sang tầng lưu trữ Processed (Parquet)."""
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.processed_dir / filename
        logger.info(f"Đang lưu dữ liệu đã transform vào tầng processed: {output_path}...")
        df.to_parquet(output_path, engine="pyarrow", index=False)
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(
            f"Lưu file processed hoàn tất: {output_path.name} "
            f"({len(df):,} dòng, {file_size_mb:.2f} MB)."
        )
        return output_path

    def load_to_sql(
        self,
        df,
        table_name="amazon_transformed",
        if_exists="replace",
        chunksize=2000,
    ):
        """
        Nạp DataFrame vào bảng SQL Server với hỗ trợ batching và tối ưu NVARCHAR.
        """
        engine = self.get_engine()
        logger.info(
            f"Bắt đầu nạp {len(df):,} dòng vào SQL Server -> bảng: [{table_name}] "
            f"(chế độ: {if_exists})..."
        )

        try:
            # Lọc dtype_map phù hợp với các cột có trong df
            dtype_mapping = {
                col: dtype
                for col, dtype in self.SQL_DTYPE_MAP.items()
                if col in df.columns
            }

            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                dtype=dtype_mapping,
            )

            # Kiểm tra và xác nhận số dòng thực tế trong DB
            with engine.connect() as conn:
                verified_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM [{table_name}]")
                ).scalar()

            logger.info(
                f"Nạp dữ liệu vào SQL Server thành công! Bảng [{table_name}] hiện có {verified_count:,} dòng."
            )
            return verified_count

        except Exception as e:
            logger.exception(f"Lỗi trong quá trình nạp dữ liệu vào bảng [{table_name}]: {e}")
            raise

    def load(self, df, table_name="amazon_transformed", save_parquet=True):
        """Điều phối bước load: lưu file processed (tùy chọn) và nạp vào database."""
        if df is None or df.empty:
            logger.warning("DataFrame rỗng, không có dữ liệu để nạp.")
            return 0

        if save_parquet:
            self.save_to_processed(df)

        loaded_rows = self.load_to_sql(df, table_name=table_name)
        return loaded_rows


def load_data(df, table_name="amazon_transformed", save_parquet=True):
    """Hàm tiện ích cấp module để gọi nhanh việc nạp dữ liệu."""
    loader = DataLoader()
    return loader.load(df, table_name=table_name, save_parquet=save_parquet)


if __name__ == "__main__":
    setup_logging(log_filename="etl_process.log")
    logger.info("Chạy trực tiếp module load.py...")
    from src.transform import transform_data

    transformed_df = transform_data()
    loaded_count = load_data(transformed_df)
    print(f"\n[LOAD HOÀN TẤT] Đã nạp thành công {loaded_count:,} dòng vào SQL Server!")
