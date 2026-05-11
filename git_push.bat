@echo off
REM ============================================================
REM  Olist ETL Pipeline — Git Setup & Push to GitHub
REM  Run this file from the project root: d:\olist-etl-project
REM ============================================================

echo === Step 1: Initialize git repository ===
git init

echo === Step 2: Stage all files (respects .gitignore) ===
git add .

echo === Step 3: Show what will be committed ===
git status --short

echo === Step 4: Create initial commit ===
git commit -m "Initial commit: Olist ETL pipeline with PySpark, DuckDB, Airflow, dbt, and Dash dashboard"

echo === Step 5: Set default branch to main ===
git branch -M main

echo === Step 6: Add remote origin ===
git remote add origin https://github.com/simegnew1213/Big_Data_olist-etl-project.git

echo === Step 7: Push to GitHub ===
git push -u origin main

echo.
echo ============================================================
echo  Done! Visit your repo at:
echo  https://github.com/simegnew1213/Big_Data_olist-etl-project
echo ============================================================
pause
