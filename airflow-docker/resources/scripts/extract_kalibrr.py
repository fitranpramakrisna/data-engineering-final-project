import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

def extract_web_kalibrr():
    base_url = "https://www.kalibrr.com/kjs/job_board/search"
    limit = 15
    batch_size = 5  # Jumlah request yang dilakukan secara paralel
    date_threshold = datetime.strptime("2024-01-01", "%Y-%m-%d")  # Batas tanggal

    # Dictionary untuk menyimpan hasil
    all_jobs = {
        "job_title": [],
        "company_name": [],
        "city": [],
        "job_posted": [],
        "job_type": [],
        "level_experience": [],
        "education_min_lvl": [],
        "industry": [],
        "min_salary": [],
        "max_salary": [],
        "source_job":[]
    }

    # Fungsi untuk mengambil data berdasarkan offset
    def fetch_jobs(offset):
        url = f"{base_url}?limit={limit}&offset={offset}&function=IT+and+Software,Sciences&country=Indonesia"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            jobs = data.get("jobs", [])

            if not jobs:  # Jika tidak ada data, stop iterasi
                return None

            extracted_jobs = {
                "job_title": [],
                "company_name": [],
                "city": [],
                "job_posted": [],
                "job_type": [],
                "level_experience": [],
                "education_min_lvl": [],
                "industry": [],
                "min_salary": [],
                "max_salary": [],
                "source_job":[]
            }

            for job in jobs:
                # Ambil tanggal posting dan konversi ke format YYYY-MM-DD
                job_posted_raw = job.get("created_at", "N/A")
                job_posted_date = "N/A"
                
                
                location_data = job.get("google_location", {}).get("address_components", {})
                country = location_data.get("country", "N/A")
                
                if country.lower() != "indonesia":  # Skip jika bukan dari Indonesia
                    continue
                
                if job_posted_raw != "N/A":
                    try:
                        job_posted_dt = datetime.strptime(job_posted_raw[:10], "%Y-%m-%d")
                        if job_posted_dt < date_threshold:
                            continue  # Skip data yang lebih lama dari 2024-12-01
                        job_posted_date = job_posted_dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass  # Jika format tanggal salah, biarkan sebagai "N/A"

                # Tambahkan data yang sudah difilter
                extracted_jobs["job_title"].append(job.get("name", "N/A"))
                extracted_jobs["company_name"].append(job.get("company_name", "N/A"))
                extracted_jobs["city"].append(
                    job.get("google_location", {}).get("address_components", {}).get("city", "N/A")
                )
                extracted_jobs["job_posted"].append(job_posted_date)
                extracted_jobs["job_type"].append(job.get("tenure", "N/A"))
                extracted_jobs["level_experience"].append(job.get("work_experience", "N/A"))
                extracted_jobs["education_min_lvl"].append(job.get("education_level", "N/A"))
                extracted_jobs["industry"].append(job.get("company", {}).get("industry", "N/A"))
                
                
                
                # extracted_jobs["min_salary"].append(job.get("base_salary", "N/A"))
                extracted_jobs["min_salary"].append(job.get("base_salary") if job.get("base_salary") is not None else "N/A")
                # extracted_jobs["max_salary"].append(job.get("maximum_salary", "N/A"))
                extracted_jobs["max_salary"].append(job.get("maximum_salary") if job.get("maximum_salary") is not None else "N/A")
                extracted_jobs["source_job"].append("Kalibrr")

            return extracted_jobs
        return None

    # Looping dengan multi-threading untuk mengambil semua data
    offset = 0
    while True:
        offsets_batch = [offset + i * limit for i in range(batch_size)]
        
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            results = executor.map(fetch_jobs, offsets_batch)

        stop = False
        for result in results:
            if result is None:
                stop = True
                break
            for key in all_jobs.keys():
                all_jobs[key].extend(result[key])  # Simpan data ke dalam dictionary utama
        
        if stop:
            break
        
        offset += batch_size * limit  # Loncat batch_size halaman

    # Konversi dictionary ke DataFrame Pandas
    df = pd.DataFrame(all_jobs)
    print(df.columns)
    print(f"total baris : {df.shape[0]}")
    df = df.to_json()
    # print(df)
    return df
    # print(df)

# print(df)

# def transform(df):
    # standarisasi nama kota
    # df['city'] = df['city'].apply(lambda x: 'Jakarta' if 'Jakarta' in x else x)
    # df['city'] = df['city'].str.replace(r'\b(Regency|Kota)\b', '', regex=True).str.strip()
    
    #standarisasi level_education
#     education_mapping = {
#     550: 'S1',
#     200: 'SMA',
#     350: 'D3',
#     450: 'D3'
# }

#     df['education_min_lvl'] = df['education_min_lvl'].map(education_mapping).fillna('Other')
    
    #standarisasi level_experience
    # working_lvl_experience_mapping = {
    #     200: 'Freshgrad / Junior',
    #     400: 'Mid-Senior Level Manager',
    #     300: 'Supervisor / Asisten Manager',
    #     100: 'Intern'

    # }
    
    # df['level_experience'] = df['level_experience'].map(working_lvl_experience_mapping).fillna('Other')
    # print(df['min_salary'].unique())
    # print(df['max_salary'].unique())
    
    # return df.to_excel('transformed_kalibrr.xlsx', index=False)

# df = extract()
# transform(df)