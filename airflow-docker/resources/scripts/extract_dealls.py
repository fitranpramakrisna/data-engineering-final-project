def extract_web_dealls():  
    import requests
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime

    base_url = "https://api.sejutacita.id/v1/explore-job/job"
    categories = ["it-and-engineering", "data-and-product"]
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
        "source_job": []
    }

    def fetch_jobs(page, category):
        url = f"{base_url}?page={page}&sortParam=mostRelevant&sortBy=asc&categorySlug={category}&published=true&limit=5&status=active"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            jobs = data.get("data", {}).get("docs", [])
            
            if not jobs:
                return None  # Berhenti jika tidak ada data
            
            extracted_jobs = {key: [] for key in all_jobs.keys()}
            
            for job in jobs:
                job_posted_raw = job.get("publishedAt", "N/A")
                job_posted_date = "N/A"

                if job_posted_raw != "N/A":
                    try:
                        job_posted_dt = datetime.strptime(job_posted_raw[:10], "%Y-%m-%d")
                        if job_posted_dt < date_threshold:
                            continue  # Skip data yang lebih lama dari 2024-01-01
                        job_posted_date = job_posted_dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass  # Jika format tanggal salah, biarkan sebagai "N/A"

                extracted_jobs["job_title"].append(job.get("role", "N/A"))
                extracted_jobs["company_name"].append(job.get("company", {}).get("name", "N/A"))
                extracted_jobs["city"].append((job.get("city") or {}).get("name", "N/A"))
                extracted_jobs["job_posted"].append(job_posted_date)
                extracted_jobs["job_type"].append(job.get("employmentTypes", ["N/A"])[0])
                extracted_jobs["level_experience"].append(job.get("candidatePreference", {}).get("lastEducations", []))
                extracted_jobs["education_min_lvl"].append("S1" if extracted_jobs["job_type"][-1] in {"contract", "fullTime"} else "SMA")
                extracted_jobs["industry"].append(job.get("company", {}).get("sector", "N/A"))
                extracted_jobs["source_job"].append("Dealls")
                
                salary_range = job.get("salaryRange", [])
                if not salary_range:
                    extracted_jobs["min_salary"].append("N/A")
                    extracted_jobs["max_salary"].append("N/A")
                else:
                    extracted_jobs["min_salary"].append(salary_range.get("start", "N/A"))
                    extracted_jobs["max_salary"].append(salary_range.get("end", "N/A"))
            
            return extracted_jobs
        return None

    for category in categories:
        page = 1
        while True:
            pages_batch = [page + i for i in range(batch_size)]
            
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                results = executor.map(lambda p: fetch_jobs(p, category), pages_batch)
            
            stop = False
            for result in results:
                if result is None:
                    stop = True
                    break
                for key in all_jobs.keys():
                    all_jobs[key].extend(result[key])
            
            if stop:
                break
            
            page += batch_size  

    df = pd.DataFrame(all_jobs)
    
    print(f"total baris : {df.shape[0]}")
    
    # print(f"Total duplikat: {df.duplicated().sum()}")
    print(df.columns)
    
    return df.to_json()
