import pandas as pd
from PIL import Image
import io
import glob
import os

# Step 1: Set the directory containing your Parquet files
parquet_directory = "./MME/data/"
# Step 2: Collect all Parquet file paths
parquet_files = glob.glob(os.path.join(parquet_directory, "*.parquet"))

print(f"Found {len(parquet_files)} Parquet files.")

# Step 3: Read each Parquet file into a DataFrame
dataframes = []
for file in parquet_files:
    print(f"Reading {file}")
    df_temp = pd.read_parquet(file)
    dataframes.append(df_temp)

# Step 4: Concatenate all DataFrames into a single DataFrame
df = pd.concat(dataframes, ignore_index=True)

# Step 5: Verify the merged DataFrame
print("Merged DataFrame Info:")
print(df.info())
print(df.head())


# Function to save images from bytes
def save_image_from_bytes(data, file_path):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Read image from bytes and save
    image = Image.open(io.BytesIO(data))
    image.save(file_path)


# Iterate over the DataFrame and save each image
for index, row in df.iterrows():
    img_data = row["image"]["bytes"]  # Extract bytes for image
    file_path = (
        f"./MME_Benchmark_release_version/{row['question_id']}"  # Construct file path
    )
    print("Saving image to", file_path)
    save_image_from_bytes(img_data, file_path)
