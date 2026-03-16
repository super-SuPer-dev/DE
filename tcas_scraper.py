import os
import requests
import pandas as pd

def download_and_consolidate_tcas():
    staging_file = os.path.join("data", "tcas_competition_rate.csv")
    
    # Check if staging file exists
    if os.path.exists(staging_file):
        print(f"Loading TCAS data from local staging layer: {staging_file}")
        return pd.read_csv(staging_file)

    print("Staging file not found. Fetching Excel files from mytcas.com...")
    
    # Mapping extracted from the provided HTML
    excel_sources = [
        {"name": "TCAS68 (รอบ 3 ครั้งที่ 2)", "url": "https://assets.mytcas.com/68/T68-stat-r3_2-maxmin-24May25.xlsx"},
        {"name": "TCAS68 (รอบ 3 ครั้งที่ 1)", "url": "https://assets.mytcas.com/68/T68-stat-r3_1-maxmin-20May25.xlsx"},
        {"name": "TCAS67", "url": "https://assets.mytcas.com/67/TCAS67_maxmin.xlsx"},
        {"name": "TCAS66", "url": "https://assets.mytcas.com/maxmin/TCAS66_maxmin.xlsx"},
        {"name": "TCAS65", "url": "https://assets.mytcas.com/maxmin/TCAS65_maxmin.xlsx"},
        {"name": "TCAS64", "url": "https://assets.mytcas.com/maxmin/TCAS64_maxmin.xlsx"},
        {"name": "TCAS63", "url": "https://assets.mytcas.com/maxmin/TCAS63_maxmin.xlsx"},
        {"name": "TCAS62", "url": "https://assets.mytcas.com/maxmin/TCAS62_maxmin.xlsx"},
    ]

    downloads_dir = os.path.join("data", "downloads_tcas")
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)

    all_data = []

    for source in excel_sources:
        file_name = source['url'].split("/")[-1]
        file_path = os.path.join(downloads_dir, file_name)
        
        # Download if not exists locally
        if not os.path.exists(file_path):
            print(f"Downloading {source['name']}...")
            try:
                response = requests.get(source['url'], timeout=30)
                if response.status_code == 200:
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                else:
                    print(f"Failed to download {source['name']}: Status {response.status_code}")
                    continue
            except Exception as e:
                print(f"Error downloading {source['name']}: {e}")
                continue

        # Process the Excel file
        print(f"Processing {source['name']}...")
        try:
            # Read excel (using openpyxl for .xlsx)
            df_year = pd.read_excel(file_path)
            df_year['tcas_round_name'] = source['name']
            all_data.append(df_year)
        except Exception as e:
            print(f"Error processing {source['name']}: {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # Save to staging layer
        final_df.to_csv(staging_file, index=False)
        print(f"Successfully consolidated {len(all_data)} files into {staging_file}")
        return final_df
    else:
        print("No data collected.")
        return pd.DataFrame()

if __name__ == "__main__":
    tcas_data = download_and_consolidate_tcas()
    if not tcas_data.empty:
        print("\nPreview of Consolidated Staging Layer:")
        print(tcas_data.head())
        print(f"\nTotal records: {len(tcas_data)}")
