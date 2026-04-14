# 🚀 HƯỚNG DẪN THU THẬP DỮ LIỆU (INGESTION) CHO ĐỒ ÁN BIG DATA

Tài liệu này cung cấp hướng dẫn chi tiết về cách thu thập dữ liệu đầu vào (Data Ingestion) cho 10 đề tài đồ án môn học Big Data. 

**Nguyên tắc cốt lõi (Bắt buộc):** Dữ liệu phải được thu thập một cách tự động thông qua code (API, Crawl, Stream), tuyệt đối không tải file thủ công bằng tay (Copy-Paste) rồi đưa vào thư mục. Dữ liệu thô thu thập được phải lưu vào thư mục `data/1_raw/`.

---

## NHÓM 1: LẤY DỮ LIỆU TĨNH TỪ KAGGLE / UCI
Áp dụng cho các đề tài không yêu cầu dữ liệu thời gian thực (Real-time).

**Danh sách đề tài áp dụng:**
1. Phân tích Giỏ hàng thông minh (Instacart Dataset)
2. Phân khúc khách hàng RFM (Online Retail II)
3. Phát hiện Gian lận Giao dịch (Credit Card Fraud Detection)
4. Hệ thống phát hiện tin giả (Fake and real news dataset)
5. Gợi ý phim/nhạc (MovieLens Dataset)

### 1. Thuật toán & Thư viện yêu cầu
* **Đề tài 1 (Giỏ hàng):** Thuật toán `FP-Growth` (Thư viện: `pyspark.ml.fpm`).
* **Đề tài 2 (RFM):** Thuật toán `K-Means Clustering` và Spark SQL (Thư viện: `pyspark.ml.clustering`).
* **Đề tài 3 (Gian lận):** Thuật toán `Random Forest Classifier` (Thư viện: `pyspark.ml.classification`). Cần kết hợp kỹ thuật xử lý dữ liệu lệch (Imbalanced data).
* **Đề tài 4 (Tin giả):** Thuật toán `Logistic Regression` hoặc `Naive Bayes` (Thư viện: `pyspark.ml.classification`).
* **Đề tài 5 (Gợi ý Phim):** Thuật toán `ALS - Alternating Least Squares` (Thư viện: `pyspark.ml.recommendation`).

### 2. Hướng dẫn Code (Dùng Kaggle API)
Thay vì click tải thủ công, các nhóm sử dụng thư viện `kaggle` của Python để tải tự động.

**Cài đặt:** `pip install kaggle`
*(Yêu cầu sinh viên tạo API Token trên trang cá nhân Kaggle và lưu file `kaggle.json` vào thư mục `~/.kaggle/` trên máy).*

**Code Ingestion (`src/ingestion_kaggle.py`):**
```python
import os
from kaggle.api.kaggle_api_extended import KaggleApi

def fetch_kaggle_data(dataset_name, output_dir):
    """Tự động tải và giải nén dataset từ Kaggle."""
    print(f"⏳ Đang tải dataset: {dataset_name}...")
    api = KaggleApi()
    api.authenticate() 
    
    os.makedirs(output_dir, exist_ok=True)
    # Tải và tự động giải nén file zip
    api.dataset_download_files(dataset_name, path=output_dir, unzip=True)
    print(f"✅ Đã tải thành công tại thư mục: {output_dir}")

if __name__ == "__main__":
    # Ví dụ lấy dữ liệu cho đề tài Gian lận giao dịch
    DATASET = "mlg-ulb/creditcardfraud" 
    RAW_DIR = "data/1_raw/fraud_data"
    fetch_kaggle_data(DATASET, RAW_DIR)
```
##NHÓM 2: DỮ LIỆU THỜI GIAN THỰC (REAL-TIME) QUA REST API
Áp dụng cho các đề tài cần xử lý luồng dữ liệu liên tục bằng Kafka và Spark Streaming.

**Danh sách đề tài áp dụng:**

1. Dự báo xu hướng chứng khoán / Crypto (API Binance / CoinGecko)
2. Phân tích cảm xúc sự kiện mạng xã hội (API Reddit / Twitter)
3. Hệ thống giám sát không khí IoT (API OpenWeatherMap)

### 1. Thuật toán & Thư viện yêu cầu
* **Đề tài Chứng khoán/Crypto:** Sử dụng Spark Streaming để tính toán các chỉ số kỹ thuật (MACD, RSI) theo cửa sổ thời gian (Window Functions).
* **Đề tài Mạng xã hội:** Sử dụng các thư viện NLP như NLTK, VADER hoặc Spark NLP để phân tích sắc thái văn bản (Tích cực/Tiêu cực/Trung tính).
* **Đề tài IoT:** Thuật toán tính toán trung bình trượt, phát hiện dị thường theo chu kỳ. Sử dụng Spark Streaming Window Functions.

### 2. Hướng dẫn Code (API -> Kafka Producer)
Sử dụng thư viện requests để gọi API và đẩy dữ liệu thẳng vào Apache Kafka.

**Cài đặt: pip install requests kafka-python**

**Code Ingestion (src/ingestion_stream.py):**

```Python
import requests
import json
import time
from kafka import KafkaProducer

def fetch_weather_and_stream(api_key, city, producer):
    """Gọi API thời tiết và đẩy vào Kafka Topic."""
    url = f"[http://api.openweathermap.org/data/2.5/weather?q=](http://api.openweathermap.org/data/2.5/weather?q=){city}&appid={api_key}"
    
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        # Lọc ra các thông số cần thiết
        payload = {
            "city": city,
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "timestamp": time.time()
        }
        
        # Đẩy dữ liệu vào Kafka topic tên là 'weather_stream'
        producer.send('weather_stream', json.dumps(payload).encode('utf-8'))
        print(f"📡 Đã đẩy dữ liệu lên Kafka: {payload}")
        
    except Exception as e:
        print(f"❌ Lỗi kết nối API: {e}")

if __name__ == "__main__":
    # Khởi tạo Kafka Producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    API_KEY = "nhap_api_key_cua_ban_vao_day" # Lưu ý: Nên dùng biến môi trường (dotenv)
    
    # Giả lập thiết bị IoT gửi dữ liệu mỗi 5 giây
    print("🚀 Bắt đầu luồng dữ liệu IoT...")
    while True:
        fetch_weather_and_stream(API_KEY, "Hanoi", producer)
        time.sleep(5)
```
##NHÓM 3: LOG SERVER VÀ WEB CRAWLING
Áp dụng cho các đề tài thu thập dữ liệu bằng cách đọc file nhật ký hệ thống liên tục hoặc bóc tách từ HTML.

**Danh sách đề tài áp dụng:**

1. Phân tích Log Server & Phát hiện tấn công DDoS (Access Log)
2. Gợi ý việc làm (Crawl dữ liệu từ các trang tuyển dụng IT)

### 1. Thuật toán & Thư viện yêu cầu
* **Đề tài DDoS:** Sử dụng Regular Expression (Regex) để bóc tách IP, Timestamp, HTTP Status từ chuỗi Log. Dùng Spark Core (reduceByKey) để đếm tần suất truy cập.
* **Đề tài Gợi ý Việc làm:** Sử dụng thuật toán TF-IDF (Đánh trọng số từ khóa) và Cosine Similarity (Đo khoảng cách tương đồng văn bản). Thư viện: pyspark.ml.feature.

### 2. Hướng dẫn Code (Đọc luồng Log / Cào Web)
**A. Đọc luồng Log Server giả lập (src/ingestion_log.py):**

```Python
import time

def tail_log_file(file_path):
    """Đọc file log theo thời gian thực (Giống lệnh tail -f)."""
    print(f"👀 Đang theo dõi file log: {file_path}")
    with open(file_path, 'r') as f:
        f.seek(0, 2) # Nhảy đến dòng cuối cùng của file
        
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5) # Đợi 0.5s nếu không có log mới
                continue
                
            # Đẩy dòng log này vào Kafka hoặc in ra
            print(f"📝 Phát hiện log mới: {line.strip()}")

if __name__ == "__main__":
    tail_log_file("/var/log/apache2/access.log")
```
**B. Cào Web Tuyển Dụng (src/ingestion_crawl.py):**

```Python
import requests
from bs4 import BeautifulSoup
import csv
import os

def scrape_job_titles(url, output_dir):
    """Cào tiêu đề công việc từ một trang web giả lập."""
    print(f"⏳ Đang cào dữ liệu từ: {url}")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Tìm các thẻ chứa tiêu đề (Ví dụ thẻ h3, class 'job-title')
    jobs = soup.find_all('h3', class_='job-title') 
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "raw_jobs.csv")
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Job Title']) # Header
        for job in jobs:
            writer.writerow([job.text.strip()])
            
    print(f"✅ Đã lưu file dữ liệu cào được tại: {output_path}")
```
**🔒 LƯU Ý BẢO MẬT QUAN TRỌNG (BẮT BUỘC ĐỌC)**
Trong quá trình gọi API, các nhóm thường phải sử dụng các API Keys, Access Tokens hoặc Passwords.
Tuyệt đối KHÔNG gán cứng (hardcode) các thông tin nhạy cảm này trực tiếp vào file code .py. Nếu đẩy lên GitHub, tài khoản của bạn có thể bị xâm nhập hoặc bị trừ tiền oan.

**Cách xử lý chuẩn:**

* Cài đặt thư viện: pip install python-dotenv

* Tạo một file tên là .env ở thư mục gốc của dự án, nhập Key vào đó:

```Plaintext
OPENWEATHER_API_KEY=12345abcdefgh67890
KAGGLE_USERNAME=ten_cua_ban
KAGGLE_KEY=ma_bi_mat
```
Đảm bảo file .gitignore ĐÃ CHẶN file .env (không cho phép push lên Git).

Sử dụng trong code Python như sau:

```Python
import os
from dotenv import load_dotenv

# Load cấu hình từ file .env
load_dotenv()

# Lấy Key ra để xài an toàn
api_key = os.getenv("OPENWEATHER_API_KEY")
```
Chúc các nhóm hoàn thành tốt giai đoạn Ingestion và xây dựng được đường ống dữ liệu vững chắc!
