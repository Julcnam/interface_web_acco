from concurrent.futures import ProcessPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import os
import tarfile
from pathlib import Path
from config import DOWNLOAD_PATH
from docx import Document
import os
from alive_progress import alive_bar
from odf.opendocument import BytesIO, load
from odf.text import P
from odf import teletype
import docx2txt
import zipfile
import pytesseract
from PIL import Image
from pdf2image import convert_from_path
from docx2pdf import convert
import pythoncom
from odt_pdf.odt_to_pdf import convert_odt_to_pdf
from tqdm import tqdm


# Chemin de téléchargement des fichiers
download_path = DOWNLOAD_PATH
pytesseract.pytesseract.tesseract_cmd = ".\\Tesseract\\tesseract.exe"

# Fonction pour s'assurer que le répertoire de téléchargement existe et lancer les téléchargements
def ensure_download_path():
    print("Ouverture de Chrome et lancement des téléchargements...")

    download_dir = Path(download_path)
    download_dir.mkdir(parents=True, exist_ok=True)
    options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": str(download_dir)}
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)
    driver.get("https://echanges.dila.gouv.fr/OPENDATA/ACCO/")

    elements = driver.find_elements(By.XPATH, "//a[contains(@href, '.tar.gz')]")
    existing_files = set(download_dir.iterdir())

    to_download = []
    for elem in elements:
        href = elem.get_attribute("href")
        filename = Path(href).name
        filename = filename.replace(".tar.gz","")
        if download_dir / filename not in existing_files:
            to_download.append(elem)

    if not to_download:
        print("Aucun nouveau fichier à télécharger.")
        driver.quit()
        return

    with alive_bar(len(to_download), title="Téléchargement...") as bar:
        for elem in to_download:
            elem.click()
            bar()
            time.sleep(0.3)  

    def wait_for_downloads():
        while any(f.suffix == ".crdownload" for f in download_dir.iterdir()):
            time.sleep(1)

    wait_for_downloads()
    print("Téléchargements terminés.")
    driver.quit()


# Fonction pour extraire les fichiers .tar.gz dans le répertoire de téléchargement
def ensure_extract_files():
    print("Extraction des fichiers...")
    download_dir = Path(download_path)
    archives = [
        a for a in download_dir.iterdir()
        if a.is_file() and a.suffixes == [".tar", ".gz"]
    ]
    
    if not archives:
        print("Aucune archive à extraire.")
        return

    with alive_bar(len(archives), title="Extraction...") as bar:
        for archive in archives:
            extract_dir = download_dir / archive.stem.replace(".tar", "")
            extract_dir.mkdir(exist_ok=True)

            with tarfile.open(archive, "r:gz") as tar:
                tar.extractall(path=extract_dir)

            archive.unlink()
            
            bar()

    print("Extraction terminée.")



def find_doc_with_only_images(path):
  
    errors= set()
    documents = []

    try:
        with zipfile.ZipFile(path, 'r') as file:
            names = file.namelist()

            text = ""
            image_count = 0
            total_text = 0

            # --- DOCX ---
            if "word/document.xml" in names:
                text = file.read("word/document.xml").decode("utf-8", errors="ignore")
                image_count = len([f for f in names if f.startswith("word/media/")])

            # --- ODT ---
            elif "content.xml" in names:
                text = file.read("content.xml").decode("utf-8", errors="ignore")
                image_count = len([f for f in names if f.startswith("Pictures/")])

            total_text = len(text.strip())

            
            if image_count > 0 and total_text < 250:
                documents.append(path)
            
        
    except Exception as err:
        print(f"Erreur {err} lors de l'analyse du fichiers odt : {path}")
        errors.add(str(path))


    if errors:
        with open("log_errors.txt", "w") as log_file:
            for error in errors:
                log_file.write(f"{error}\n")

    return documents      
        

def ocr_documents_with_only_images(path):
    pythoncom.CoInitialize()
    print(f"Traitement OCR du document : {path})")
    if path.suffix == ".docx":
        convert(path, path.with_suffix(".pdf"))
    elif path.suffix == ".odt":
        convert_odt_to_pdf(path, path.with_suffix(".pdf"))

    text_pages = convert_from_path(path.with_suffix(".pdf"),dpi=200,poppler_path="poppler\\Library\\bin")

    extracted_text = []
    for page in text_pages:
        text = pytesseract.image_to_string(page, config="--oem 3 --psm 6").strip()
        extracted_text.append(text)

    with open(path.with_suffix(".txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(extracted_text))

    path.with_suffix(".pdf").unlink(missing_ok=True)
    path.unlink(missing_ok=True)

    pythoncom.CoUninitialize()
                
                                

def docx_to_txt(path):
    txt_path = path.with_suffix(".txt")
    try:
        # Vérification de l'existence du fichier
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Fichier introuvable : {path}")

        text = docx2txt.process(path)
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)
      
        path.unlink()
    except Exception as err:
        print(f"Erreur lors de la conversion : {err}")
        

def odt_to_txt(path):       
    txt_path = path.with_suffix(".txt")
    try:
        # Lecture du fichier ODT
        doc = load(path)
        paragraphs = doc.getElementsByType(P)

        full_text = []

        # Extraction du texte
        for para in paragraphs:
            text = teletype.extractText(para)
            if text.strip():
                full_text.append(text)


        # Écriture dans le fichier TXT
        with open(txt_path, "w", encoding="utf-8") as txt_file:
            txt_file.write("\n".join(full_text))
        
        path.unlink()
    except Exception as err:
        print(f"Erreur lors de la conversion : {err}")
        
        
        
def ensure_conversion_txt():
   
    base_path = Path(DOWNLOAD_PATH)
    docx_files = list(base_path.rglob("*.docx"))
    odt_files = list(base_path.rglob("*.odt"))
    doc_paths = docx_files + odt_files

   
    # Filtrage
    with ProcessPoolExecutor() as executor:
        docs = list(tqdm(executor.map(find_doc_with_only_images, doc_paths,chunksize=10),total=len(doc_paths),desc="Filtrage des documents"))

    docs = [d for d in docs if d]  # IMPORTANT


    #  OCR
    with ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(ocr_documents_with_only_images, docs,chunksize=10),total=len(docs),desc="Lancement de l'OCR"))
        
    # 

    # docx_files = [
    #     path for path in base_path.rglob("*.docx")
    #     if path.is_file() and not path.with_suffix(".txt").exists()
    # ]

    # odt_files = [
    #     path for path in base_path.rglob("*.odt")
    #     if path.is_file() and not path.with_suffix(".txt").exists()
    # ]



    if not docx_files and not odt_files:
        print("Aucun fichier à convertir.")
        return

    with ProcessPoolExecutor() as executor:
        list(tqdm(executor.map(docx_to_txt,str(docx_files),chunksize=10), total=len(docx_files), desc="Conversion docx ..."))
         
            
    with ProcessPoolExecutor() as executor:
        list(tqdm(executor.map(odt_to_txt,str(odt_files),chunksize=10), total=len(odt_files), desc="Conversion odt ..."))
         
            
    
    print(f"Conversion terminée.")

 