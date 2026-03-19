import subprocess


def convert_odt_to_pdf(input_file: str, output_file: str) -> None:
    """
    Convert an ODT file to a PDF file using LibreOffice.
    If the file already exists, it will be overwritten.

    Args:
        input_file (str): The path to the input ODT file.
        output_file (str): The path to save the output PDF file.

    Returns:
        None
    """
    libreoffice = "C:\\Program Files\\LibreOffice\\program\\soffice.exe"
    subprocess.run(
        [
            libreoffice,
            "--headless",
            "--convert-to",
            "pdf",
            input_file,
            "--outdir",
            output_file,
        ]
    )


if __name__ == "__main__":
    input_file = "11135961_RELINT_SEPOL.odt"
    output_directory = "."

    convert_odt_to_pdf(input_file, output_directory)
