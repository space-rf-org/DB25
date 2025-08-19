#!/bin/bash

# Build script for LaTeX documentation
# Requires: pdflatex, tikz, pgfplots packages

echo "Building Query Engine Architecture Documentation..."

# Create docs directory if it doesn't exist
mkdir -p /Users/chiradip/codes/lib_pg_cpp/docs

# Change to docs directory
cd /Users/chiradip/codes/lib_pg_cpp/docs

# Build PDF (run multiple times for references and ToC)
echo "Running pdflatex (first pass)..."
pdflatex -interaction=nonstopmode query_engine_architecture.tex

echo "Running pdflatex (second pass for references)..."
pdflatex -interaction=nonstopmode query_engine_architecture.tex

echo "Running pdflatex (third pass for ToC)..."
pdflatex -interaction=nonstopmode query_engine_architecture.tex

# Clean up auxiliary files
echo "Cleaning up auxiliary files..."
rm -f *.aux *.log *.out *.toc *.fls *.fdb_latexmk *.synctex.gz

echo "PDF generated: query_engine_architecture.pdf"

# Check if PDF was created successfully
if [ -f "query_engine_architecture.pdf" ]; then
    echo "✅ Success! Documentation built successfully."
    echo "📄 Output: $(pwd)/query_engine_architecture.pdf"
    
    # Get file size
    size=$(du -h query_engine_architecture.pdf | cut -f1)
    echo "📊 File size: $size"
    
    # Count pages (requires pdftk or similar)
    if command -v pdfinfo >/dev/null 2>&1; then
        pages=$(pdfinfo query_engine_architecture.pdf | grep Pages | awk '{print $2}')
        echo "📖 Pages: $pages"
    fi
else
    echo "❌ Error: PDF generation failed!"
    exit 1
fi