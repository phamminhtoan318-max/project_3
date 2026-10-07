"""
Module: main.py
Mục đích: Điểm khởi chạy chính của toàn bộ quy trình ETL (Extract - Transform - Load).
Thiết kế theo mô hình hướng đối tượng (OOP) giúp code rõ ràng, dễ bảo trì và dễ giám sát.
"""

import sys
import time
import logging
from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config
from src.logger import setup_logging
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data, DataLoader


class AmazonETLPipeline:
    """
    Lớp điều phối toàn bộ chu trình ETL dữ liệu Amazon:
    - E (Extract)  : Trích xuất CSV từ tầng Raw sang Parquet ở tầng Staging.
    - T (Transform): Làm sạch chuỗi review dính chùm, chuẩn hóa text, tính chỉ số tài chính.
    - L (Load)     : Nạp dữ liệu vào kho lưu trữ Processed (Parquet) và Data Warehouse (SQL Server).
    """

    def __init__(
        self,
        raw_filename="amazon.csv",
        staging_filename="amazon.parquet",
        target_table="amazon_transformed",
        log_filename="etl_process.log",
    ):
        self.raw_filename = raw_filename
        self.staging_filename = staging_filename
        self.target_table = target_table
        self.log_filename = log_filename

        # Cài đặt logger
        setup_logging(log_filename=self.log_filename)
        self.logger = logging.getLogger(self.__class__.__name__)

        # Khởi tạo DataLoader riêng
        self.data_loader = DataLoader()

        # Biến theo dõi hiệu năng và kết quả
        self.metrics = {
            "extract_rows": 0,
            "transform_rows": 0,
            "loaded_rows": 0,
            "duration_seconds": 0.0,
            "status": "CHƯA CHẠY",
        }
        self.start_time = None

    def _print_step_banner(self, step_number, step_name, description):
        """In thanh tiêu đề phân tách từng bước rõ ràng trên console."""
        print(f"\n{'='*70}")
        print(f" >>> [BƯỚC {step_number}/3] {step_name.upper()}: {description}")
        print(f"{'='*70}")

    def extract(self):
        """Thực hiện bước Extract: Đọc CSV raw và lưu vào Staging."""
        self._print_step_banner(
            1, "Extract", f"Trích xuất '{self.raw_filename}' -> Staging Parquet"
        )
        self.logger.info("Bắt đầu bước EXTRACT...")

        raw_df = extract_data(file_name=self.raw_filename)
        self.metrics["extract_rows"] = len(raw_df)
        self.logger.info(f"Hoàn thành EXTRACT: {len(raw_df):,} dòng.")
        return raw_df

    def transform(self):
        """Thực hiện bước Transform: Xử lý dữ liệu từ Staging."""
        self._print_step_banner(
            2,
            "Transform",
            "Tách chuỗi review dính chùm, làm sạch số liệu & tính chỉ số tài chính",
        )
        self.logger.info("Bắt đầu bước TRANSFORM...")

        transformed_df = transform_data()
        self.metrics["transform_rows"] = len(transformed_df)
        self.logger.info(
            f"Hoàn thành TRANSFORM: {len(transformed_df):,} dòng, {len(transformed_df.columns)} cột."
        )
        return transformed_df

    def load(self, df):
        """Thực hiện bước Load: Nạp vào SQL Server và lưu tầng Processed."""
        self._print_step_banner(
            3,
            "Load",
            f"Nạp dữ liệu vào SQL Server [{config.DB_NAME}].[{self.target_table}]",
        )
        self.logger.info("Bắt đầu bước LOAD...")

        loaded_count = self.data_loader.load(
            df=df,
            table_name=self.target_table,
            save_parquet=True,
        )
        self.metrics["loaded_rows"] = loaded_count
        self.logger.info(
            f"Hoàn thành LOAD: {loaded_count:,} dòng đã được nạp vào SQL Server."
        )
        return loaded_count

    def print_summary(self):
        """Hiển thị bảng tổng kết toàn diện, trực quan sau khi hoàn tất pipeline."""
        print("\n" + "=" * 70)
        print("               BÁO CÁO TỔNG KẾT QUY TRÌNH ETL (SUMMARY)               ")
        print("=" * 70)
        print(f" * Trạng thái thực thi   : {self.metrics['status']}")
        print(f" * Tổng thời gian chạy   : {self.metrics['duration_seconds']:.2f} giây")
        print(f" * Dữ liệu đầu vào (Raw) : {self.metrics['extract_rows']:,} dòng")
        print(
            f" * Dữ liệu sau biến đổi  : {self.metrics['transform_rows']:,} dòng (đã phân rã review)"
        )
        print(f" * Dữ liệu nạp vào DB    : {self.metrics['loaded_rows']:,} dòng")
        print(f" * Database đích         : {config.DB_NAME} (SQL Server)")
        print(f" * Bảng dữ liệu đích     : {self.target_table}")
        print(
            f" * File Parquet lưu trữ  : data/processed/{self.target_table}.parquet"
        )
        print("=" * 70 + "\n")

    def run(self):
        """
        Hàm chính điều phối toàn bộ luồng ETL:
        Extract -> Transform -> Load
        """
        print("\n" + "#" * 70)
        print("            KHỞI CHẠY PIPELINE ETL DỮ LIỆU AMAZON                     ")
        print("#" * 70)

        self.start_time = time.time()

        try:
            # 1. Trích xuất (Extract)
            self.extract()

            # 2. Biến đổi (Transform)
            transformed_df = self.transform()

            # 3. Nạp (Load)
            self.load(transformed_df)

            # Cập nhật kết quả thành công
            self.metrics["status"] = "THÀNH CÔNG (SUCCESS)"
            self.metrics["duration_seconds"] = time.time() - self.start_time

            # In báo cáo tổng kết
            self.print_summary()
            self.logger.info("Toàn bộ quy trình ETL hoàn thành xuất sắc!")
            return True

        except Exception as e:
            self.metrics["status"] = f"THẤT BẠI (FAILED: {e})"
            if self.start_time:
                self.metrics["duration_seconds"] = time.time() - self.start_time

            self.print_summary()
            self.logger.exception(f"Pipeline gặp sự cố nghiêm trọng: {e}")
            return False


if __name__ == "__main__":
    # Khởi tạo và thực thi ETL Pipeline
    pipeline = AmazonETLPipeline()
    is_success = pipeline.run()

    if not is_success:
        sys.exit(1)
