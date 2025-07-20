# **ETL DATA PIPELINE OPTIMIZATION** 

## Overview
Proyek ETL Data Pipeline Optimization ini bertujuan untuk membangun sebuah pipeline data end-to-end yang dapat mengintegrasikan berbagai sumber data tambang, termasuk:
    - Log Produksi Harian (berasal dari file SQL dump),
    - Sensor Alat Berat (dari file CSV),
    - Data Cuaca (menggunakan API dari Open-Meteo).

Pipeline ini akan:
Extract data dari sumber-sumber di atas, Transform data menjadi berbagai metrik analitis penting, seperti:
- Total produksi harian (total_production_daily)
- Rata-rata kualitas batubara (average_quality_grade)
- Utilisasi alat (equipment_utilization)
- Efisiensi bahan bakar (fuel_efficiency)
- Dampak cuaca terhadap produksi (weather_impact)

Load hasil akhirnya ke dalam ClickHouse, yaitu database OLAP yang cepat dan cocok untuk analisis skala besar.

Seluruh proses berjalan otomatis dan dapat dijalankan di lingkungan lokal menggunakan Docker untuk memastikan replikasi yang konsisten.

Pipeline ini juga terhubung ke Metabase untuk menampilkan dashboard visualisasi interaktif, sehingga tim analis atau manajemen bisa mengambil keputusan berdasarkan data yang sudah bersih dan terintegrasi.

## How to use the Project
1. Clone Repository 
   ```
    git clone https://github.com/ahmadarbain/data_mining_optimization.git
   ```
   selanjutnya masuk ke direktori project
   ```
   cd data_mining_optimization
   ```

2. Installasi Docker
    Jika belum, kamu bisa install Docker Desktop di:
    https://www.docker.com/products/docker-desktop

3. Jalankan docker-compose.yaml
    Pada tahap ini jalankan perintah berikut untuk menggunakan clickhouse dan metabase pada docker 
    ```
    docker-compose up -d
    ```
    Ini akan menjalankan:
    - ClickHouse (Database OLAP)
    - Metabase (Visualisasi Dashboard)
    
4. Siapkan Environment Project (venv)
    Gunakan Python 3.9, misalnya dengan pyenv atau venv.
    install pypi
    ```
        pip install py
        py -3.9 -m venv venv
        
        # aktifkan env 
        source venv/bin/activate  # Linux/Mac
        venv\Scripts\activate     # Windows
        
        # installasi requirements di dalam env
        pip install -r requirements.txt
    ```

5. Running etl proses
    Jalankan perintah berikut untuk menjalankan seluruh proses ETL
    ```
    python main.py
    ```
    
6. Struktur Proyek
    ```
    .
    ├── datasets/                   # Folder dataset mentah
    ├── clickhouse/                 # Konfigurasi Docker untuk ClickHouse
    ├── metabase/                   # Konfigurasi Docker untuk Metabase
    ├── src/
    │   ├── usecase/
    │   │   └── daily_production.py  # Proses utama ETL
    │   └── interface/
    │       └── database.py          # Abstraksi untuk koneksi database
    ├── requirements.txt
    ├── docker-compose.yml
    └── main.py
    ```