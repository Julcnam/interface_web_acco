"""Celery task to generate a PDF from a LibreOffice document (mostly ODT)."""
__version__ = "1.0.4"

from logging import getLogger
from celery import shared_task
import subprocess
from os import getenv
from odt_to_pdf.conversion import file_to_string, string_to_file
from tempfile import mkdtemp
import re
from pathlib import Path

LIBRE_OFFICE = getenv('LIBRE_OFFICE_BINARY', '/usr/bin/libreoffice')
LOGGER = getLogger(__name__)

@shared_task
def convert_odt_to_pdf(utf8_decoded: str='', source_file_path: str='', output_directory: str='', odt_command_options=None, keep_odt=False, keep_pdf=False):
  if not utf8_decoded and not source_file_path:
    raise Exception('Either provide a path to the ODT or its utf8 decoded content')
  if not source_file_path:
    temp_source_file = string_to_file(utf8_decoded)
    LOGGER.info('ODT file saved to %s', temp_source_file.name)
  else:
    # TODO get a file wrapper around the original file if that's provided
    temp_source_file = None
    
  output_directory = output_directory or mkdtemp()
  LOGGER.info('PDF will be output to %s', output_directory)
  odt_command_options = odt_command_options or []

  command = [LIBRE_OFFICE, "--headless", "--convert-to", "pdf", temp_source_file.name, "--outdir", output_directory] + odt_command_options

  LOGGER.info('Running following command: %s', ' '.join(command))

  try:
    output = subprocess.check_output(command)
  except subprocess.CalledProcessError as ex:
    LOGGER.error('An error occurred while transforming to PDF: %s', ex)
    raise ex
  else:
    LOGGER.info('Output of PDF conversion command %s', output)

  odt_path = Path(temp_source_file.name)
  odt_name = odt_path.name
  # Libreoffice keeps the same name just changes the 
  pdf_filename = re.sub(r"\.odt$", ".pdf", odt_name)
  LOGGER.info('PDF file %s', pdf_filename)

  pdf_file_path = Path(output_directory) / pdf_filename
  LOGGER.info('PDF path %s', pdf_file_path)

  as_string = file_to_string(pdf_file_path)
  # Clean up
  if not keep_odt:
    odt_path.unlink()
  if not keep_pdf:
    pdf_file_path.unlink()

  return as_string
  
