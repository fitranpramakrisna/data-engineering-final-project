# import requests

# url = 'https://api.sejutacita.id/v1/explore-job/job?page=1&sortParam=mostRelevant&sortBy=asc&categorySlug=it-and-engineering&published=true&limit=5&status=active&externalPlatformApplyUrlSet=null'

# response = requests.get(url)

# data = response.json()
# jobs = data.get("data", {}).get("docs", [])

# all_jobs = {
#     "job_title": [],
#     "company_name": [],
#     "city": [],
#     "job_posted": [],
#     "job_type": [],
#     "level_experience": [],
#     "education_min_lvl": [],
#     "industry": [],
#     "min_salary": [],
#     "max_salary": []
# }

# for job in jobs:
#     job_title = job.get("role", "N/A")
#     company_name = job.get("company", []).get("name", "N/A")
#     city = job.get("city", {}).get("name", "N/A")
#     job_posted = job.get("publishedAt", "N/A")
#     job_type = job.get("employmentTypes")[0]
#     # level_experience = job.get("candidatePreference", []).get("lastEducation", "N/A")
#     level_experience = job.get("candidatePreference", {}).get("lastEducations", [])

#     # if level_experience == [7]:
#     #     experience = "min. 5+ years of experience"
#     # elif level_experience == [6, 5]:
#     #     experience = "min. 1 year of experience"
#     # elif level_experience == [6, 7]:
#     #     experience = "min. 4 years of experience"
#     # elif level_experience == [0]:
#     #     experience = "freshgrad"
#     # elif level_experience == [0, 4]:
#     #     experience = "Min. 4th Year College Student"
#     # else:
#     #     experience = "other"

    
#     education_min_lvl = 'S1' if job_type in {'contractual', 'fullTime'} else 'SMA'
#     industry = job.get("company", []).get("sector", "N/A")
    
#     if not job.get("salaryRange", []):
#         max_salary = min_salary = 'Undisclosed'
#     else: 
#         min_salary = job.get("salaryRange", []).get("start", "N/A")
#         max_salary = job.get("salaryRange", []).get("end", "N/A")
    
#     print(level_experience)
#     # print(experience)
#     print(job_title)
#     print(company_name)
#     print(job_posted)
#     print(job_type)
#     print(min_salary)
#     print(max_salary)
#     print(education_min_lvl)
    
#     print("<===========================>")



# import requests

# url = 'https://api.sejutacita.id/v1/explore-job/job?page=1&sortParam=mostRelevant&sortBy=asc&categorySlug=it-and-engineering&published=true&limit=5&status=active&externalPlatformApplyUrlSet=null'

# response = requests.get(url)

# data = response.json()
# jobs = data.get("data", {}).get("docs", [])

# all_jobs = {
#     "job_title": [],
#     "company_name": [],
#     "city": [],
#     "job_posted": [],
#     "job_type": [],
#     "level_experience": [],
#     "education_min_lvl": [],
#     "industry": [],
#     "min_salary": [],
#     "max_salary": []
# }

# for job in jobs:
#     job_title = job.get("role", "N/A")
#     company_name = job.get("company", []).get("name", "N/A")
#     city = job.get("city", {}).get("name", "N/A")
#     print(city)
#     job_posted = job.get("publishedAt", "N/A")
#     job_type = job.get("employmentTypes")[0]
#     # level_experience = job.get("candidatePreference", []).get("lastEducation", "N/A")
#     level_experience = job.get("candidatePreference", {}).get("lastEducations", [])

    # if level_experience == [7]:
    #     experience = "min. 5+ years of experience"
    # elif level_experience == [6, 5]:
    #     experience = "min. 1 year of experience"
    # elif level_experience == [6, 7]:
    #     experience = "min. 4 years of experience"
    # elif level_experience == [0]:
    #     experience = "freshgrad"
    # elif level_experience == [0, 4]:
    #     experience = "Min. 4th Year College Student"
    # else:
    #     experience = "other"

    
    # education_min_lvl = 'S1' if job_type in {'contractual', 'fullTime'} else 'SMA'
    # industry = job.get("company", []).get("sector", "N/A")
    
    # if not job.get("salaryRange", []):
    #     max_salary = min_salary = 'Undisclosed'
    # else: 
    #     min_salary = job.get("salaryRange", []).get("start", "N/A")
    #     max_salary = job.get("salaryRange", []).get("end", "N/A")
    
    
def extract_web_dealls():  
    import requests
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime

    base_url = "https://api.sejutacita.id/v1/explore-job/job"
    categories = ["it-and-engineering", "data-and-product"]
    batch_size = 5  # Jumlah request yang dilakukan secara paralel

    date_threshold = datetime.strptime("2024-12-01", "%Y-%m-%d")  # Batas tanggal

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
        "max_salary": []
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
                            continue  # Skip data yang lebih lama dari 2024-12-01
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
                
                if not job.get("salaryRange", []):
                    # max_salary = min_salary = 'Undisclosed'
                    extracted_jobs["min_salary"].append( "N/A")
                    extracted_jobs["max_salary"].append( "N/A")
                else: 
                    # min_salary = job.get("salaryRange", []).get("start", "N/A")
                    # max_salary = job.get("salaryRange", []).get("end", "N/A")
                # salary_range = job.get("salaryRange", [])
                    extracted_jobs["min_salary"].append(job.get("salaryRange", []).get("start", "N/A"))
                    extracted_jobs["max_salary"].append(job.get("salaryRange", []).get("end", "N/A"))
            
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
            
            page += batch_size  # Loncat batch_size halaman

    df = pd.DataFrame(all_jobs)
    print(f"total baris : {df.shape[0]}")
    
    # return df.to_json()
    df = df.to_json()
    # print(df)
    return df
    # print(df)

# df.to_excel("dealls_jobs_filtered.xlsx", index=False)
# print(df)

# def transform(df):
#     df['city'] = df['city'].apply(lambda x: 'Jakarta' if 'Jakarta' in x else x)
#     df['city'] = df['city'].str.replace(r'\b(Regency|Kota)\b', '', regex=True).str.strip()
#     df['job_typ'] = df['city'].apply(lambda x: 'Jakarta' if 'Jakarta' in x else x)

    
    #standarisasi level_experience
    # working_lvl_experience_mapping = {
    #     200: 'Freshgrad / Junior',
    #     400: 'Mid-Senior Level Manager',
    #     300: 'Supervisor / Asisten Manager',
    #     100: 'Intern'

    # }
    
    # df['level_experience'] = df['level_experience'].map(working_lvl_experience_mapping).fillna('Other')
    
    # def map_experience(row):
    #     min_exp = min(row['level_experience']) 

    #     if row['job_type'] == 'internship':
    #         return 'Intern'
    #     elif min_exp in [0, 5]:
    #         return 'Fresh Grad / Junior'
    #     elif min_exp == 6:
    #         return 'Supervisor / Asisten Manager'
    #     elif min_exp == 7:
    #         return 'Mid-Senior Level Manager'
    #     else:
    #         return 'Other'
        
    # df['level_experience'] = df.apply(map_experience, axis=1)
    
    # with open("industry_job_kalibrr.txt", "r", encoding="utf-8") as file:
    #     industry_job_kalibrr = [line.strip() for line in file.readlines()]
    
    # def match_industry(industry):
    #     from fuzzywuzzy import process
    #     best_match, score = process.extractOne(industry, industry_job_kalibrr)
    #     return best_match if score > 95 else industry  # Hanya ubah jika kecocokan > 80%
    
    # df['industry'] = df['industry'].apply(match_industry)
    
    # print(df['industry'])
    # return df.to_excel("dealls_jobs_filtered_v4.xlsx", index=False)
    # print(df)
    
    
    
# df = extract()
# transform(df)