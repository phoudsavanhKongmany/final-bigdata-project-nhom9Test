import os
import pytest

# ==========================================
# TEST 3: KIỂM TRA BẢO MẬT & AN TOÀN DỮ LIỆU
# ==========================================

def test_gitignore_chan_thu_muc_data():
    """Kiểm tra xem sinh viên đã chặn đẩy dữ liệu lên Git chưa."""
    gitignore_path = '.gitignore'
    
    # Bỏ qua test nếu file không tồn tại (đã bị báo lỗi ở Test 1)
    if not os.path.isfile(gitignore_path):
        pytest.skip("Không tìm thấy .gitignore, bỏ qua test này.")
        
    with open(gitignore_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Kiểm tra xem có chữ 'data/' hoặc '*.csv' trong file cấu hình không
    is_blocking_data = 'data/' in content or 'data' in content
    
    error_msg = (
        "❌ LỖI NGHIÊM TRỌNG: File .gitignore chưa chặn thư mục 'data/'. "
        "Hãy thêm chữ 'data/' vào file .gitignore để tránh đẩy dữ liệu nặng lên GitHub!"
    )
    assert is_blocking_data, error_msg

def test_thu_muc_data_khong_bi_push_len_git():
    """
    Kiểm tra xem sinh viên có lỡ push file dữ liệu (csv, parquet) lên repo không.
    (Giới hạn kiểm tra ở cấp độ thư mục gốc và thư mục data nếu có).
    """
    # Tìm tất cả file csv, json, parquet (bỏ qua thư mục requirements nếu có)
    data_files = []
    forbidden_extensions = ('.csv', '.parquet', '.avro')
    
    for root, dirs, files in os.walk('.'):
        # Bỏ qua các thư mục môi trường, git ảo
        if '.git' in root or 'venv' in root or '.pytest_cache' in root:
            continue
        for file in files:
            if file.endswith(forbidden_extensions):
                data_files.append(os.path.join(root, file))
                
    error_msg = (
        f"❌ LỖI: Phát hiện dữ liệu {data_files} bị đẩy lên GitHub!\n"
        "-> Yêu cầu: Xóa file khỏi Git và cấu hình lại .gitignore."
    )
    assert len(data_files) == 0, error_msg
