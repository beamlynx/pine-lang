#!/bin/sh

echo "Starting Pine initialization..."

for i in $(seq 1 20); do
  echo "Attempt $i: Testing Pine connection..."
  if curl -s -f "http://pine:33333/api/v1/connection" > /dev/null 2>&1; then
    echo "Pine is ready! Registering database connection..."
    CONNECTION_RESPONSE=$(curl -s -X POST "http://pine:33333/api/v1/connections" \
      -H "Content-Type: application/json" \
      -d "{
        \"host\": \"sample-db-ecommerce\",
        \"port\": 5432,
        \"dbname\": \"pine\",
        \"user\": \"pine_readonly\",
        \"password\": \"pine_readonly\",
        \"schema\": \"public\"
      }")
    
    echo "Connection response: $CONNECTION_RESPONSE"
    
    # Extract the connection-id value from the JSON response
    CONNECTION_ID=$(echo "$CONNECTION_RESPONSE" | grep -o '"connection-id":"[^"]*"' | cut -d'"' -f4)
    
    if [ -z "$CONNECTION_ID" ]; then
      echo "Failed to extract connection ID from response"
      exit 1
    fi
    
    echo "Created connection with ID: $CONNECTION_ID"
    
    CONNECT_RESPONSE=$(curl -s -X POST "http://pine:33333/api/v1/connections/$CONNECTION_ID/connect")
    echo "Connect response: $CONNECT_RESPONSE"
    
    curl -s "http://pine:33333/api/v1/connections"
    echo "Database connection registered and connected!"
    exit 0
  fi
  echo "Pine not ready yet, waiting 2 seconds..."
  sleep 2
done

echo "Failed to connect to Pine after 10 attempts"
exit 1 