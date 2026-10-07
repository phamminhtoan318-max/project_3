import sys
import re
import unicodedata
import logging
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config
from src.logger import setup_logging

logger = logging.getLogger(__name__)
staging_path = config.STAGING_DIR

INVALID_TOKENS = {"", "none", "null", "nan", "n/a", "na", "unknown", "undefined", "?"}
_INVISIBLE = re.compile(r"[\u200B-\u200D\uFEFF]")

# Giả định giá vốn = 70% giá gốc (dataset Amazon không có cost thật)
ESTIMATED_COST_RATIO = 0.70

# Các cột dạng "a,b,c" cần tách ra nhiều dòng
LIST_REVIEW_COLS = [
    "user_id",
    "user_name",
    "review_id",
    "review_title",
    "review_content",
]

# Cột ID: chỉ strip, không đổi nội dung
ID_COLS = ["product_id", "user_id", "review_id"]

# Text cấp sản phẩm: thiếu -> "Unknown"
PRODUCT_TEXT_COLS = ["product_name", "category", "about_product"]

# Text cấp review: thiếu -> giữ NULL (không bịa "Unknown" để khỏi lẫn với dữ liệu thật)
REVIEW_TEXT_COLS = ["user_name", "review_title", "review_content"]

CURRENCY_COLS = ["discounted_price", "actual_price"]
PERCENT_COLS = ["discount_percentage"]


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def load_parquet_data(filename="amazon.parquet", source_dir=staging_path):
    input_path = Path(filename) if Path(filename).is_absolute() else source_dir / filename
    if not input_path.exists():
        error_msg = f"Không tìm thấy file dữ liệu nguồn tại: {input_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    logger.info(f"Bắt đầu đọc dữ liệu từ staging: {input_path}")
    try:
        df = pd.read_parquet(input_path)
        logger.info(f"Đọc thành công {len(df):,} dòng và {len(df.columns)} cột từ {input_path.name}.")
        return df
    except Exception as e:
        logger.exception(f"Lỗi xảy ra trong quá trình đọc dữ liệu: {e}")
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def is_invalid(val):
    """True nếu giá trị rỗng/null/token rác. An toàn với list/array."""
    if isinstance(val, (list, tuple, np.ndarray)):
        return len(val) == 0
    if isinstance(val, str):
        return val.strip().lower() in INVALID_TOKENS
    return val is None or pd.isna(val)


def clean_text_data(data_text, default="Unknown"):
    if is_invalid(data_text):
        return default

    text = str(data_text)
    if not text.isascii():
        text = unicodedata.normalize("NFC", text)

    text = _INVISIBLE.sub("", text)
    cleaned = " ".join(text.split())
    return cleaned if cleaned else default


def _split_csv(val):
    """Tách 1 ô thành list. Ô null -> [None]; đã là list thì giữ nguyên."""
    if isinstance(val, (list, tuple, np.ndarray)):
        return list(val)
    if is_invalid(val):
        return [None]
    return [s.strip() for s in str(val).split(",")]


# ---------------------------------------------------------------------------
# Transform steps
# ---------------------------------------------------------------------------
def explode_review_lists(df):
    """
    Tách các cột review dạng 'a,b,c' thành nhiều dòng.

    Cột nào có số phần tử KHÔNG khớp với review_id (vd: review_content có dấu
    phẩy trong câu) thì set NULL cho dòng đó + log warning, thay vì cắt/pad
    làm lệch nội dung sang review khác.
    """
    df = df.copy()
    cols = [c for c in LIST_REVIEW_COLS if c in df.columns]
    if not cols:
        return df

    base = "review_id" if "review_id" in cols else cols[0]
    split = {c: df[c].map(_split_csv) for c in cols}
    n = split[base].map(len)

    for c in cols:
        lens = split[c].map(len)
        ok = lens == n
        bad = int((~ok).sum())
        if bad:
            logger.warning(
                f"Cột '{c}': {bad:,} dòng lệch độ dài với '{base}' -> set NULL "
                f"(thường do dấu phẩy nằm trong nội dung text)."
            )
        df[c] = [
            items if is_ok else [None] * k
            for items, is_ok, k in zip(split[c], ok, n)
        ]

    return df.explode(cols, ignore_index=True)


def clean_id_columns(df, cols=ID_COLS):
    """Strip khoảng trắng, token rác -> NA. Chạy trước khi dedup."""
    df = df.copy()
    for col in cols:
        if col in df.columns:
            s = df[col].astype("string").str.strip()
            df[col] = s.mask(s.str.lower().isin(INVALID_TOKENS))
    return df


def clean_duplicates(df, subset_cols=None):
    initial_rows = len(df)
    df_cleaned = df.drop_duplicates(subset=subset_cols, keep="last")
    removed = initial_rows - len(df_cleaned)
    if removed > 0:
        logger.info(f"Đã loại bỏ {removed:,} dòng trùng lặp (theo subset: {subset_cols}).")
    return df_cleaned.reset_index(drop=True)


def standardize_text(df):
    df = df.copy()
    for col in PRODUCT_TEXT_COLS:
        if col in df.columns:
            df[col] = df[col].apply(clean_text_data)
    for col in REVIEW_TEXT_COLS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: clean_text_data(v, default=None))
    return df


def clean_currency_columns(df, cols=CURRENCY_COLS):
    """Parse tiền tệ. Lỗi parse -> NaN (không ép về 0 để khỏi méo thống kê)."""
    df = df.copy()
    for col in cols:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            cleaned = df[col].astype(str).str.replace(r"[₹$,\s]", "", regex=True)
            df[col] = pd.to_numeric(cleaned, errors="coerce")
    return df


def clean_percentage_columns(df, cols=PERCENT_COLS):
    """Giữ thang 0-100 (vd: 64 = 64%)."""
    df = df.copy()
    for col in cols:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            cleaned = df[col].astype(str).str.replace("%", "", regex=False).str.strip()
            df[col] = pd.to_numeric(cleaned, errors="coerce")
    return df


def clean_numeric_columns(df):
    df = df.copy()

    if "rating" in df.columns:
        # Dataset Amazon có 1 dòng rating = "|" -> NaN (thiếu rating != rating 0)
        cleaned = df["rating"].astype(str).str.replace("|", "", regex=False).str.strip()
        df["rating"] = pd.to_numeric(cleaned, errors="coerce")

    if "rating_count" in df.columns:
        if not pd.api.types.is_numeric_dtype(df["rating_count"]):
            df["rating_count"] = (
                df["rating_count"].astype(str).str.replace(",", "", regex=False).str.strip()
            )
        # Đếm thiếu = 0 lượt đánh giá là hợp lý
        df["rating_count"] = (
            pd.to_numeric(df["rating_count"], errors="coerce").fillna(0).astype("int64")
        )

    return df


def calculate_financial_metrics(df):
    """
    Tính discount_amount và các chỉ số lợi nhuận ƯỚC TÍNH.
    Dataset không có giá vốn thật nên estimated_* chỉ là giả định.
    Giá thiếu (NaN) -> kết quả NaN, không giả 0.
    """
    df = df.copy()

    if "actual_price" in df.columns and "discounted_price" in df.columns:
        df["discount_amount"] = (df["actual_price"] - df["discounted_price"]).clip(lower=0)
    else:
        df["discount_amount"] = np.nan

    if "cost_price" in df.columns:
        cost = df["cost_price"]
    elif "actual_price" in df.columns:
        logger.warning(
            f"Không có cột 'cost_price'. Giả lập estimated_cost_price = actual_price * {ESTIMATED_COST_RATIO}"
        )
        cost = df["actual_price"] * ESTIMATED_COST_RATIO
    else:
        cost = None

    if cost is not None and "discounted_price" in df.columns:
        df["estimated_cost_price"] = cost.round(2)
        df["estimated_profit"] = (df["discounted_price"] - df["estimated_cost_price"]).round(2)

        # Thang 0-100 cho đồng nhất với discount_percentage
        valid = df["discounted_price"] > 0
        df["estimated_profit_margin"] = np.where(
            valid,
            (df["estimated_profit"] / df["discounted_price"] * 100).round(2),
            np.nan,
        )

    return df


def handle_missing_values(df):
    """
    Chỉ fill những cột mà 0 thật sự có nghĩa (đếm).
    rating / giá / profit thiếu thì GIỮ NULL.
    """
    df = df.copy()

    if "rating_count" in df.columns:
        df["rating_count"] = df["rating_count"].fillna(0).astype("int64")

    for col in PRODUCT_TEXT_COLS:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    # Cờ để downstream biết dòng nào thiếu rating
    if "rating" in df.columns:
        df["is_rating_missing"] = df["rating"].isna()

    return df


def clean_staging_data(raw_df):
    dedup_cols = ["product_id"]
    if "review_id" in raw_df.columns:
        dedup_cols.append("review_id")

    cleaned_df = (
        raw_df
        .pipe(explode_review_lists)                         # 1. Tách review dính chùm (an toàn)
        .pipe(clean_id_columns)                             # 2. Strip ID trước khi dedup
        .pipe(clean_duplicates, subset_cols=dedup_cols)     # 3. Xóa trùng theo đúng grain
        .pipe(standardize_text)                             # 4. Chuẩn hóa NFC / text
        .pipe(clean_currency_columns, cols=CURRENCY_COLS)   # 5. Ép kiểu tiền tệ
        .pipe(clean_percentage_columns, cols=PERCENT_COLS)  # 6. Ép kiểu %
        .pipe(clean_numeric_columns)                        # 7. Ép kiểu rating/count
        .pipe(calculate_financial_metrics)                  # 8. Tính discount + estimated profit
        .pipe(handle_missing_values)                        # 9. Fill chọn lọc + cờ missing
    )
    return cleaned_df


def transform_data():
    raw_df = load_parquet_data()
    cleaned_df = clean_staging_data(raw_df)
    logger.info(
        f"Transform hoàn tất: {len(cleaned_df):,} dòng, "
        f"{len(cleaned_df.columns)} cột."
    )
    return cleaned_df


if __name__ == "__main__":
    setup_logging(log_filename="etl_process.log")
    logger.info("Chạy trực tiếp module transform.py...")
    df_transformed = transform_data()

    print("\n--- KẾT QUẢ TRANSFORM DỮ LIỆU ---")
    print("Kích thước dữ liệu (Rows, Cols):", df_transformed.shape)
    print("\nCác cột tài chính vừa xử lý và tính toán:")
    print(
        df_transformed[
            ["product_id", "actual_price", "discounted_price",
             "estimated_profit", "estimated_profit_margin"]
        ].head(5)
    )
    print("\nSố NULL mỗi cột quan trọng:")
    print(
        df_transformed[
            ["rating", "actual_price", "discounted_price", "review_id", "review_content"]
        ].isna().sum()
    )