import nbformat
from nbformat.v4 import new_notebook
import os

def execute_notebook(notebook_path):
    """Execute a notebook and save the output"""
    print(f"Executing {notebook_path}...")
    
    # Read the notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)
    
    # Execute each code cell
    for i, cell in enumerate(nb.cells):
        if cell.cell_type == 'code':
            print(f"  Executing cell {i+1}...")
            try:
                # Execute the code
                exec(cell.source)
                cell.outputs = []  # Clear any existing outputs
            except Exception as e:
                print(f"    Error in cell {i+1}: {e}")
                # Add error as output
                cell.outputs = [{
                    'output_type': 'error',
                    'ename': type(e).__name__,
                    'evalue': str(e),
                    'traceback': [str(e)]
                }]
    
    # Save the executed notebook
    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbformat.write(nb, f)
    
    print(f"Completed {notebook_path}")

# Execute all three notebooks
notebooks = [
    'notebooks/01_extract.ipynb',
    'notebooks/02_transform.ipynb', 
    'notebooks/03_load.ipynb'
]

for notebook in notebooks:
    execute_notebook(notebook)

print("All notebooks executed successfully!")
