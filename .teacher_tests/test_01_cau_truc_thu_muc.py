import os
import pytest

# ==========================================
# TEST 1: KIỂM TRA CẤU TRÚC ĐỒ ÁN
# ==========================================

def test_cac_thu_muc_bat_buoc_ton_tai():
    """Kiểm tra xem sinh viên có xóa nhầm thư mục chuẩn không."""
    required_dirs = ['src', 'docs', 'tests_nghiep_vu', 'notebooks']
    missing_dirs = []
    
    for d in required_dirs:
        if not os.path.isdir(d):
            missing_dirs.append(d)
            
    assert len(missing_dirs) == 0, f"❌ LỖI: Thiếu các thư mục bắt buộc sau: {missing_dirs}. Hãy tạo lại cho đúng tên!"

def test_cac_file_cau_hinh_ton_tai():
    """Kiểm tra xem sinh viên có nộp đủ file thông tin không."""
    required_files = ['requirements.txt', 'README.md', '.gitignore']
    missing_files = []
    
    for f in required_files:
        if not os.path.isfile(f):
            missing_files.append(f)
            
    assert len(missing_files) == 0, f"❌ LỖI: Chưa nộp các file cấu hình quan trọng: {missing_files}"
