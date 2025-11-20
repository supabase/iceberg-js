#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
CLEANUP=${CLEANUP:-false}
WAIT_TIMEOUT=${WAIT_TIMEOUT:-60}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --cleanup)
      CLEANUP=true
      shift
      ;;
    --wait-timeout)
      WAIT_TIMEOUT="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--cleanup] [--wait-timeout SECONDS]"
      exit 1
      ;;
  esac
done

echo -e "${GREEN}Starting integration test...${NC}\n"

# Function to cleanup on exit
cleanup() {
  if [ "$CLEANUP" = true ]; then
    echo -e "\n${YELLOW}Cleaning up Docker containers...${NC}"
    docker compose down -v
    echo -e "${GREEN}Cleanup complete${NC}"
  else
    echo -e "\n${YELLOW}Docker containers are still running. Use 'docker compose down -v' to stop them.${NC}"
  fi
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Start docker compose services
echo -e "${GREEN}Starting Docker services...${NC}"
docker compose up -d

# Wait for iceberg-rest to be healthy
echo -e "${GREEN}Waiting for Iceberg REST Catalog to be ready...${NC}"
SECONDS_WAITED=0
until curl -s http://localhost:8181/v1/config > /dev/null 2>&1; do
  if [ $SECONDS_WAITED -ge $WAIT_TIMEOUT ]; then
    echo -e "${RED}Timeout waiting for Iceberg REST Catalog to start${NC}"
    docker compose logs iceberg-rest
    exit 1
  fi
  echo "Waiting for catalog... ($SECONDS_WAITED/$WAIT_TIMEOUT seconds)"
  sleep 2
  SECONDS_WAITED=$((SECONDS_WAITED + 2))
done

echo -e "${GREEN}Iceberg REST Catalog is ready!${NC}\n"

# Wait a bit more for MinIO to be fully ready
echo -e "${GREEN}Waiting for MinIO to be ready...${NC}"
sleep 3

# Run the integration test
echo -e "${GREEN}Running integration test...${NC}\n"
npx tsx test/integration/test-local-catalog.ts

TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
  echo -e "\n${GREEN}✨ Integration tests passed!${NC}"
else
  echo -e "\n${RED}❌ Integration tests failed${NC}"
  exit $TEST_EXIT_CODE
fi

exit 0
