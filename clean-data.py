import pandas as pd

# Load your CSV file with proper options
df = pd.read_csv('/Users/bilalmughal/Downloads/final-customers.csv', low_memory=False)

# First, let's check what columns are available
print("Available columns in the CSV:")
print(list(df.columns))
print(f"\nDataFrame shape: {df.shape}")

# Check if we have column H, or if we need to use column indices
if 'H' in df.columns:
    source_col = 'H'
    target_col = 'I'
elif len(df.columns) > 7:  # Column H would be index 7 (0-based)
    source_col = df.columns[7]  # 8th column (H)
    target_col = df.columns[8] if len(df.columns) > 8 else 'new_phone_col'  # 9th column (I)
else:
    print("Error: Not enough columns in the CSV file")
    print(f"Expected at least 8 columns, but found {len(df.columns)}")
    exit()

print(f"\nUsing source column: '{source_col}'")
print(f"Using target column: '{target_col}'")

# If target column doesn't exist, create it
if target_col not in df.columns:
    df[target_col] = None

# Define a function to check if a value is a phone number
def is_phone(value):
    if pd.isna(value):
        return False
    value = str(value).strip()
    return value.startswith('254') or value.startswith('07')

# Count phone numbers found
phone_count = 0

# Loop through the rows
for index, value in df[source_col].items():
    if is_phone(value):
        df.at[index, target_col] = value   # Move phone number to target column
        df.at[index, source_col] = None    # Clear from source column
        phone_count += 1

# Save the cleaned data to a new CSV file
df.to_csv('/Users/bilalmughal/Downloads/cleaned_customers.csv', index=False)

print(f"\nPhone numbers moved from column '{source_col}' to column '{target_col}' successfully!")
print("New CSV file saved as: cleaned_customers.csv")
print(f"Total rows processed: {len(df)}")
print(f"Phone numbers moved: {phone_count}")