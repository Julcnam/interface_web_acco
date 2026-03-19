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



def find_doc_with_only_images():
    base_path = Path(DOWNLOAD_PATH)
    docx_files = list(base_path.rglob("*.docx"))
    odt_files = list(base_path.rglob("*.odt"))

    documents = []
    
    with alive_bar(len(docx_files), title="Analyse des fichiers docx...") as bar:
        for path in docx_files:
            total_text = 0
            image_count = 0
            paragraphs_with_only_images = 0

            doc = Document(path)
            for para in doc.paragraphs:
                text = para.text.strip()
                total_text += len(text)

                has_image = any(run.element.xpath('.//*[local-name()="drawing"]') for run in para.runs)

                if has_image:
                        image_count += 1

                if has_image and not text:
                    paragraphs_with_only_images += 1
                    
            if image_count > 0 and total_text < 250:
                documents.append((path))
            
            bar()

    with alive_bar(len(odt_files), title="Analyse des fichiers odt...") as bar:
        for path in odt_files:
            with zipfile.ZipFile(path, 'r') as file:

                text = ""
                if "content.xml" in file.namelist():
                    text = file.read("content.xml").decode("utf-8", errors="ignore")

                total_text = len(text.strip())

                
                image_count = len([f for f in file.namelist() if f.startswith("Pictures/")])

                if image_count > 0 and total_text < 250:
                    documents.append((path))

                bar()

    return documents      
        

def ocr_documents_with_only_images(documents):
    with alive_bar(len(documents), title="OCR des fichiers contenant uniquement des images...") as bar:
        for path in documents:
            print(f"Traitement du fichier : {path}")
            pythoncom.CoInitialize()
            if path.suffix == ".docx":
                convert(path, path.with_suffix(".pdf"))
            elif path.suffix == ".odt":
                convert_odt_to_pdf(path, path.with_suffix(".pdf"))

            text_pages = convert_from_path(path.with_suffix(".pdf"), dpi=300,poppler_path="poppler\\Library\\bin")
            extracted_text = []
            for page in text_pages:
                text = pytesseract.image_to_string(page).strip()
                extracted_text.append(text)
            
            with open(path.with_suffix(".txt"), "w", encoding="utf-8") as f:
                f.write("\n".join(extracted_text))
                    
                
                pythoncom.CoUninitialize()
                path.with_suffix(".pdf").unlink()
                path.unlink()


            bar()
                
                                

def docx_to_txt(docx_path, txt_path):
    try:
        # Vérification de l'existence du fichier
        if not os.path.isfile(docx_path):
            raise FileNotFoundError(f"Fichier introuvable : {docx_path}")

        text = docx2txt.process(docx_path)
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)
      
    except Exception as err:
        print(f"Erreur lors de la conversion : {err}")
        

def odt_to_txt(odt_path, txt_path):       
    try:
        # Lecture du fichier ODT
        doc = load(odt_path)
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

    except Exception as err:
        print(f"Erreur lors de la conversion : {err}")
        
        
        
def ensure_conversion_txt():
    cpt_fichier=0
    cpt_doublon=0
    cpt_vide=0
    doublon = set()
    base_path = Path(DOWNLOAD_PATH)

    documents_with_only_images = find_doc_with_only_images()
    ocr_documents_with_only_images(documents_with_only_images)


    docx_files = [
        path for path in base_path.rglob("*.docx")
        if path.is_file() and not path.with_suffix(".txt").exists()
    ]

    odt_files = [
        path for path in base_path.rglob("*.odt")
        if path.is_file() and not path.with_suffix(".txt").exists()
    ]


    if not docx_files and not odt_files:
        print("Aucun fichier à convertir.")
        return

    with alive_bar(len(docx_files), title="Conversion docx ...") as bar:
        for path in docx_files:
            txt_path = path.with_suffix(".txt")
            docx_to_txt(str(path), str(txt_path))
            path.unlink()
            bar()

    with alive_bar(len(odt_files), title="Conversion odt ...") as bar:
        for path in odt_files:
            txt_path = path.with_suffix(".txt")
            odt_to_txt(str(path), str(txt_path))
            path.unlink()
            bar()

    
    for path in base_path.rglob("*")  :

        if path.is_file() and path.suffix == ".txt":
            cpt_fichier+=1
        
        if path.name in doublon:
            cpt_doublon+=1
        else:
            doublon.add(path.name)

        if  path.is_file and path.stat().st_size < 250:
            cpt_vide+=1
            

    print(f"Conversion terminée, nombre de fichiers: {cpt_fichier}, nombre de doublons : {cpt_doublon}, nombre de fichiers vide : {cpt_vide}")
           