#!/bin/bash
# Build script for production deployment
# Builds the frontend and prepares the backend

set -e

echo "=== Building Frontend ==="
cd frontend
npm install
npm run build
echo "Frontend built to frontend/dist/"

echo ""
echo "=== Installing Backend Dependencies ==="
cd ../backend
pip install -r requirements.txt
echo "Backend ready."

echo ""
echo "=== Build Complete ==="
echo "Start with: cd backend && uvicorn app.main:app --host 0.0.0.0 --port 8000"
