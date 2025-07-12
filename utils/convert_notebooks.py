import os
import subprocess
from dotenv import load_dotenv


def convert_notebooks():
    load_dotenv()
    analysis_folder = os.getenv('ANALYSIS_FOLDER')
    src_dir = os.path.join(analysis_folder, 'src')
    pdf_dir = os.path.join(analysis_folder, 'pdf')
    html_dir = os.path.join(analysis_folder, 'html')
    # Ensure output directories exist
    os.makedirs(pdf_dir, exist_ok=True)
    os.makedirs(html_dir, exist_ok=True)
    # Loop through all .ipynb files in the src directory
    for filename in os.listdir(src_dir):
        if filename.endswith(".ipynb"):
            notebook_path = os.path.join(src_dir, filename)
            notebook_name = os.path.splitext(filename)[0]
            print(f"Converting {filename}...")
            # Convert to HTML
            subprocess.run([
                "jupyter", "nbconvert",
                "--to", "html",
                "--no-input",
                "--output", notebook_name,
                "--output-dir", html_dir,
                notebook_path
            ], check=True)
            # Convert to PDF
            subprocess.run([
                "jupyter", "nbconvert",
                "--to", "pdf",
                "--no-input",
                "--output", notebook_name,
                "--output-dir", pdf_dir,
                notebook_path
            ], check=True)
    print("✅ Conversion completed successfully.")


if __name__ == "__main__":
    convert_notebooks()
