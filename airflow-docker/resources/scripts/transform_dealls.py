def tranform_web_dealls(df):
    import pandas as pd
    print(df)
    df = pd.read_json(df)
    print(df['city'])
    # df['city'] = df['city'].apply(lambda x: 'Jakarta' if 'Jakarta' in x else x)
    df['city'] = df['city'].apply(lambda x: 'Jakarta' if isinstance(x, str) and 'Jakarta' in x else x)
    df['city'] = df['city'].str.replace(r'\b(Regency|Kota)\b', '', regex=True).str.strip()
    # df['job_typ'] = df['city'].apply(lambda x: 'Jakarta' if 'Jakarta' in x else x)

    
    # standarisasi level_experience
    # working_lvl_experience_mapping = {
    #     200: 'Freshgrad / Junior',
    #     400: 'Mid-Senior Level Manager',
    #     300: 'Supervisor / Asisten Manager',
    #     100: 'Intern'

    # }
    
    # df['level_experience'] = df['level_experience'].map(working_lvl_experience_mapping).fillna('Other')
    
    def map_experience(row):
        min_exp = min(row['level_experience']) 

        if row['job_type'] == 'internship':
            return 'Intern'
        elif min_exp in [0, 5]:
            return 'Fresh Grad / Junior'
        elif min_exp == 6:
            return 'Supervisor / Asisten Manager'
        elif min_exp == 7:
            return 'Mid-Senior Level Manager'
        else:
            return 'Other'
        
    df['level_experience'] = df.apply(map_experience, axis=1)
    
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
    print(f"total baris : {df.shape[0]}")
    return df.to_json()