import pandas as pd
import os
import asyncio
import aiohttp
from utils import process_sheet
from config import input_path, output_dir


# --- MAIN EVENT LOOP ---
async def main():
    excel_file = pd.ExcelFile(input_path)
    csv_files = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for sheet_name in excel_file.sheet_names:
            df = excel_file.parse(sheet_name)
            task = process_sheet(sheet_name, df, session)
            tasks.append(task)

        csv_files = await asyncio.gather(*tasks)

    # Combine all CSVs into one
    # combined_df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
    # combined_csv_path = os.path.join(output_dir, "combined_output.csv")
    # combined_df.to_csv(combined_csv_path, index=False)

    print("All sheets processed and exported successfully.")

# --- ENTRY POINT ---
if __name__ == "__main__":
    asyncio.run(main())