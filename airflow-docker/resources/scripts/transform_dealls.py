def tranform_web_dealls(df):
    import pandas as pd

    df = pd.read_json(df)

    df['city'] = df['city'].apply(lambda x: 'Jakarta' if isinstance(x, str) and 'Jakarta' in x else x)
    df['city'] = df['city'].apply(lambda x: 'Tangerang' if isinstance(x, str) and 'Tangerang' in x else x)
    df['city'] = df['city'].str.replace(r'\b(Regency|Kota|Kabupaten)\b', '', regex=True).str.strip()
    
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
    return df.to_json()