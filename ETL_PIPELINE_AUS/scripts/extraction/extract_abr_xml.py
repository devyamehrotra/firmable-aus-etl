import os
import requests
import zipfile
import xml.etree.ElementTree as ET
import csv

# Robust path resolution
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
ZIP_PATH = os.path.join(PROJECT_ROOT, 'data', 'abr', 'raw', 'public_split_1_10.zip')
EXTRACT_DIR = os.path.join(PROJECT_ROOT, 'data', 'abr', 'raw', 'xmls')
OUTPUT_CSV = os.path.join(PROJECT_ROOT, 'data', 'abr', 'raw', 'companies.csv')

# Configuration
ZIP_URL = "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip"

def download_zip():
    os.makedirs("data", exist_ok=True)

    if os.path.exists(ZIP_PATH):
        print(f"📦 ZIP already exists at {ZIP_PATH}. Skipping download.")
        return

    print(f"📥 Downloading ABR ZIP from: {ZIP_URL}")
    response = requests.get(ZIP_URL, stream=True)
    if response.status_code != 200:
        raise Exception("❌ Failed to download ZIP.")

    with open(ZIP_PATH, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    print(f"✅ Saved ZIP to: {ZIP_PATH}")

def unzip_files():
    print(f"🗂️  Extracting ZIP to {EXTRACT_DIR}...")
    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        zip_ref.extractall(EXTRACT_DIR)
    print(f"✅ Extraction complete.")

def get_entity_name(entity):
    # Try LegalEntity -> IndividualName
    name = entity.find(".//LegalEntity/IndividualName")
    if name is not None:
        given_names = name.findall("GivenName")
        full_name = " ".join([n.text for n in given_names if n.text])
        family = name.findtext("FamilyName", default="")
        return f"{full_name} {family}".strip()
    # Try MainEntity -> NonIndividualName
    name = entity.find(".//MainEntity/NonIndividualName")
    if name is not None:
        return name.findtext("NonIndividualNameText", default="")
    # Try EntityName, MainName, LegalName (organization fallback)
    for tag in [".//EntityName", ".//MainName", ".//LegalName"]:
        name = entity.findtext(tag)
        if name:
            return name
    return ""

def get_address(entity):
    # Try BusinessAddress/AddressDetails
    address = entity.find(".//BusinessAddress/AddressDetails")
    if address is not None:
        state = address.findtext("State", default="")
        postcode = address.findtext("Postcode", default="")
        return state, postcode
    # Try MainBusinessPhysicalAddress
    address = entity.find(".//MainBusinessPhysicalAddress")
    if address is not None:
        state = address.findtext("StateCode", default="")
        postcode = address.findtext("Postcode", default="")
        return state, postcode
    return "", ""

def extract_with_fallback(entity):
    abn_elem = entity.find(".//ABN")
    abn = abn_elem.text if abn_elem is not None else ""
    abn_status = abn_elem.attrib.get("status", "") if abn_elem is not None else ""
    entity_name = get_entity_name(entity)
    entity_type = entity.findtext(".//EntityType/EntityTypeText", default="")
    start_date = entity.findtext(".//ABNStatusFromDate", default="")
    if not start_date:
        start_date = entity.findtext(".//EntityStatus/EffectiveFrom", default="")
    state, postcode = get_address(entity)
    address = entity.findtext(".//MainBusinessPhysicalAddress/AddressLine", default="")

    # Fallback extraction if any critical field is missing
    if not abn:
        abn = entity.findtext(".//ABN", default="")
    if not entity_name:
        entity_name = get_entity_name(entity)
    if not entity_type:
        entity_type = entity.findtext(".//EntityType/EntityTypeText", default="")
    if not abn_status:
        abn_status_elem = entity.find(".//ABN")
        abn_status = abn_status_elem.attrib.get("status", "") if abn_status_elem is not None else ""
    if not start_date:
        start_date = entity.findtext(".//ABNStatusFromDate", default="")
    if not address:
        address = entity.findtext(".//MainBusinessPhysicalAddress/AddressLine", default="")
    if not postcode or not state:
        state, postcode = get_address(entity)

    return {
        "abn": abn,
        "entity_name": entity_name,
        "entity_type": entity_type,
        "entity_status": abn_status,
        "address": address,
        "postcode": postcode,
        "state": state,
        "start_date": start_date
    }

def parse_xml(file_path, writer):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        for entity in root.findall(".//ABR"):
            abn = entity.findtext(".//ABN", default="")
            entity_name = get_entity_name(entity)
            entity_type = entity.findtext(".//EntityType/EntityTypeText", default="")
            entity_status = entity.find(".//ABN")
            entity_status = entity_status.attrib.get("status", "") if entity_status is not None else ""
            start_date = entity.findtext(".//ABNStatusFromDate", default="")

            state, postcode = get_address(entity)

            writer.writerow({
                "abn": abn,
                "entity_name": entity_name,
                "entity_type": entity_type,
                "entity_status": entity_status,
                "address": f"{state} {postcode}".strip(),
                "postcode": postcode,
                "state": state,
                "start_date": start_date
            })

    except Exception as e:
        print(f"⚠️ Error parsing {file_path}: {e}")

def extract_abr_data():
    download_zip()
    unzip_files()

    os.makedirs("data", exist_ok=True)

    print(f"📤 Writing extracted data to: {OUTPUT_CSV}")
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "abn", "entity_name", "entity_type", "entity_status",
            "address", "postcode", "state", "start_date"
        ])
        writer.writeheader()

        for file in os.listdir(EXTRACT_DIR):
            if file.endswith(".xml"):
                parse_xml(os.path.join(EXTRACT_DIR, file), writer)

    print("🎉 Extraction complete!")

if __name__ == "__main__":
    extract_abr_data()
