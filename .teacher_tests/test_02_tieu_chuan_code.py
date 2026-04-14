import os
import glob
import pytest

# ==========================================
# TEST 2: KIỂM TRA CHUẨN MỰC VIẾT CODE
# ==========================================

def test_khong_co_notebook_trong_src():
    """Đảm bảo sinh viên không để file .ipynb lộn xộn trong thư mục code thuần."""
    # Quét tất cả file .ipynb nằm trong thư mục src và các thư mục con của nó
    notebooks_in_src = glob.glob('src/**/*.ipynb', recursive=True)
    
    error_msg = (
        f"❌ LỖI KỶ LUẬT: Tìm thấy file Notebook trong thư mục src/: {notebooks_in_src}\n"
        "-> Yêu cầu: Di chuyển các file .ipynb sang thư mục 'notebooks/'."
    )
    assert len(notebooks_in_src) == 0, error_msg

def test_co_file_main_pipeline():
    """Đảm bảo có file khởi chạy toàn bộ hệ thống."""
    main_file = 'src/main_pipeline.py'
    assert os.path.isfile(main_file), "❌ LỖI: Thiếu file 'src/main_pipeline.py' - Không biết chạy chương trình từ đâu!"

def test_bao_cao_pdf_ton_tai():
    """Khuyến khích (hoặc bắt buộc) sinh viên xuất báo cáo ra file PDF."""
    pdf_files = glob.glob('docs/*.pdf')
    assert len(pdf_files) > 0, "❌ LỖI: Chưa thấy nộp báo cáo cuối khóa (định dạng .pdf) trong thư mục 'docs/'"
