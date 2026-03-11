#!/bin/bash
set -e

# Start backend server
echo "Starting FastAPI Backend..."
cd backend
# Install backend dependencies
if [ -f "pyproject.toml" ]; then
  uv sync
fi
uv run uvicorn api:app --reload --port 8123 &
BACKEND_PID=$!

# Start frontend server
echo "Starting Vite Frontend..."
cd ../frontend
# Install frontend dependencies
npm install
npm run dev &
FRONTEND_PID=$!

echo "Both servers are running..."
echo "Backend: http://localhost:8123"
echo "Frontend: http://localhost:5176"

function cleanup() {
  echo "Shutting down servers..."
  kill $BACKEND_PID || true
  kill $FRONTEND_PID || true
  exit
}

trap cleanup EXIT INT TERM
wait
