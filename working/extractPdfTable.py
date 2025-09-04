import fitz  
# PyMuPDF
import pandas as pd
import re

# Load the PDF file
doc = fitz.open("FRTC_Trees_outside_Forests_Field_Manual_3.pdf")

# Extract text from pages containing Annex II (pages 13 to 61 inclusive)
annex_text = ""
for page_num in range(12, 61):  # 0-based index
    page = doc.load_page(page_num)
    annex_text += page.get_text()

# Define regex pattern to extract species data
pattern = re.compile(
    r"(?P<Code>\d{4,})\s+(?P<Scientific_Name>.+?)\s{2,}(?P<Common_Name>.+?)\s{2,}(?P<Family>.+?)\s{2,}(?P<Form>\w+)\s+(?P<Nepal>\w+)\s+(?P<Altitude>[\d\*\-–]+)\s+(?P<Local_Name>.+)"
)

# Find all matches
matches = pattern.findall(annex_text)

# Convert matches to DataFrame
columns = ["Code", "Scientific Name", "Common Name", "Family", "Form", "Nepal", "Altitude", "Local Name"]
data = [dict(zip(columns, match)) for match in matches]
df = pd.DataFrame(data)

# Save to Excel
excel_file = "Annex_II_Species_Codes.xlsx"
df.to_excel(excel_file, index=False)

print(f"Extracted {len(df)} species entries and saved to {excel_file}.")

