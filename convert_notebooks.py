import subprocess
from pathlib import Path
import argparse


def convert_notebooks(input_dir, html_output_dir, pdf_output_dir):
    input_dir = Path(input_dir)
    html_output_dir = Path(html_output_dir)
    pdf_output_dir = Path(pdf_output_dir)

    # Create output directories if they don't exist
    html_output_dir.mkdir(parents=True, exist_ok=True)
    pdf_output_dir.mkdir(parents=True, exist_ok=True)

    # Loop through all .ipynb files in the input directory
    for notebook in input_dir.glob("*.ipynb"):
        print(f"Converting {notebook.name}...")

        # Convert to HTML
        subprocess.run([
            "jupyter", "nbconvert",
            "--to", "html",
            "--no-input",
            "--output", notebook.stem,
            "--output-dir", str(html_output_dir),
            str(notebook)
        ], check=True)

        # Convert to PDF
        subprocess.run([
            "jupyter", "nbconvert",
            "--to", "pdf",
            "--no-input",
            "--output", notebook.stem,
            "--output-dir", str(pdf_output_dir),
            str(notebook)
        ], check=True)

    print("✅ Conversion completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert .ipynb notebooks to HTML and PDF using nbconvert")
    parser.add_argument("input_dir", help="Folder containing .ipynb notebooks")
    parser.add_argument("html_output_dir", help="Folder to save HTML files")
    parser.add_argument("pdf_output_dir", help="Folder to save PDF files")

    args = parser.parse_args()
    convert_notebooks(args.input_dir, args.html_output_dir,
                      args.pdf_output_dir)
