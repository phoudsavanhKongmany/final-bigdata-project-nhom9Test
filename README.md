# ĐỒ ÁN CUỐI KHÓA - MÔN HỌC BIG DATA

Chào mừng các bạn đến với Đồ án cuối khóa! Đây không chỉ là một bài tập trên lớp, mà là cơ hội để các bạn xây dựng một **Data Pipeline chuẩn Production**, sẵn sàng đưa vào CV ứng tuyển các vị trí Data Engineer / Data Analyst.

Để đảm bảo hệ thống chấm điểm tự động (CI/CD) hoạt động chính xác và công bằng, toàn bộ các nhóm **BẮT BUỘC** phải tuân thủ nghiêm ngặt các quy định dưới đây.

---

## 🎯 1. YÊU CẦU CỐT LÕI (THE BIG DATA PIPELINE)

Để đồ án đạt điểm cao, hệ thống của các bạn phải chứng minh được luồng xử lý qua 4 bước chuẩn mực:

1. **Ingestion (Thu thập):** Bắt buộc phải chứng minh dữ liệu được lấy tự động (qua API, Web Crawling, Kafka hoặc Script). Tuyệt đối KHÔNG "copy paste" file thủ công bằng tay.
2. **Storage (Lưu trữ):** Dữ liệu phải được lưu xuống hệ thống (HDFS, MinIO) hoặc lưu ở Local dưới định dạng `Parquet`. (Đây là điểm cộng lớn thể hiện sự chuyên nghiệp).
3. **Processing (Xử lý):** Code xử lý nghiệp vụ phải sử dụng `Spark DataFrame` (PySpark). Hạn chế tối đa việc dùng Pandas vì Pandas không phải là công cụ xử lý dữ liệu phân tán (Big Data).
4. **Visualization (Hiển thị):** Không chỉ in kết quả ra màn hình console.Hãy kết nối kết quả cuối cùng với Google Looker Studio, PowerBI, hoặc trực quan hóa bằng Python (Seaborn/Matplotlib).

---

## 📂 2. CẤU TRÚC THƯ MỤC CHUẨN

Repository của nhóm phải giữ nguyên cấu trúc dưới đây. **Tuyệt đối không xóa đổi tên các thư mục có sẵn:**

```text
📦 DO_AN_BIGDATA
 ┣ 📂 .teacher_tests/      # (GIỮ NGUYÊN) Chứa bộ test tự động của Giảng viên
 ┣ 📂 data/                # (KHÔNG PUSH LÊN GIT) Nơi lưu dữ liệu (Theo chuẩn Bronze-Silver-Gold)
 ┣ 📂 docs/                # Nơi nộp file báo cáo (.pdf), slide thuyết trình
 ┣ 📂 notebooks/           # Nơi chứa các file .ipynb dùng để nháp, vẽ biểu đồ (EDA)
 ┣ 📂 src/                 # Nơi chứa MÃ NGUỒN CHÍNH (các file .py như ingestion.py, processing.py)
 ┣ 📂 tests_nghiep_vu/     # Nơi sinh viên TỰ VIẾT các Unit Test để bảo vệ code của mình
 ┣ 📜 .gitignore           # File cấu hình chặn đẩy dữ liệu rác lên Git
 ┣ 📜 README.md            # File này
 ┗ 📜 requirements.txt     # Khai báo các thư viện cần cài đặt (ví dụ: pyspark, kafka-python...)
```
## 🚫 3. NHỮNG "ĐIỀU CẤM KỴ" (SẼ BỊ TRỪ ĐIỂM NẶNG)CẤM PUSH DỮ LIỆU LÊN GITHUB: 
* Thư mục data/ đã được chặn trong file .gitignore. Các bạn chỉ chạy và lưu dữ liệu trên máy tính cá nhân/Colab. Nếu cố tình đẩy file .csv, .parquet nặng lên làm phình/treo Repository, nhóm sẽ bị trừ điểm sàn ngay lập tức.
* CẤM HARDCODE API KEY / MẬT KHẨU: Tuyệt đối không viết thẳng mật khẩu hoặc API Key vào file code .py. Hãy dùng file .env (được cấu hình ẩn) và thư viện python-dotenv để load key.
* KHÔNG VIẾT CODE CHÍNH TRONG NOTEBOOK: Thư mục src/ chỉ nhận file .py. File .ipynb chỉ được phép nằm trong thư mục notebooks/ để phân tích dữ liệu (EDA).
## 🤖 4. QUY TRÌNH CHẤM ĐIỂM TỰ ĐỘNG (AUTOGRADING) Đồ án này áp dụng quy trình kiểm thử liên tục (CI/CD). 
Mỗi khi bạn nộp code (Push lên GitHub nhánh main), hệ thống GitHub Actions sẽ tự động chạy 2 vòng kiểm tra:
* Vòng 1 (Test Giảng Viên): Quét xem nhóm có nộp đủ báo cáo không, code có sai chuẩn không, có lỡ push file data lên không. (Bắt buộc Pass để lấy điểm sàn).
* Vòng 2 (Test Sinh Viên): Tự động chạy tất cả các file test do chính các bạn viết trong thư mục tests_nghiep_vu/.👉 Hãy theo dõi tab "Actions" trên GitHub để xem code của mình có được Tích Xanh (Passed) hay không nhé!
## 📋 5. DANH SÁCH ĐỀ TÀI ĐỊNH HƯỚNG
Tùy thuộc vào nhóm đăng ký, các bạn sẽ thực hiện 1 trong 5 hướng đề tài sau:
* Nhóm 1: Thương mại điện tử & Bán lẻ (Market Basket Analysis, RFM).
* Nhóm 2: Tài chính & Ngân hàng (Fraud Detection, Stock Trend Prediction).
* Nhóm 3: Mạng xã hội & NLP (Real-time Sentiment Analysis, Fake News Detection).
* Nhóm 4: IoT & Thành phố thông minh (Air Quality, DDoS Detection).
* Nhóm 5: Hệ thống gợi ý (Movie/Music, Job Recommendation).
Chúc các nhóm phân công công việc hiệu quả và hoàn thành xuất sắc đồ án!
