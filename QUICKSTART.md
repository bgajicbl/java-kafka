# Quick Start Verification Guide

Follow these steps in order to verify your local environment is set up correctly.

## Step 1: Clean Start Infrastructure (5 minutes)

```bash
cd /Users/bojan/IdeaProjects/java-kafka

# Remove any previous containers
docker compose down

# Start fresh
docker compose up -d

# Wait for services to be ready
sleep 10

# Verify all containers are healthy
docker compose ps
```

**Expected Output:**
All services should show `Up` status:
- `zookeeper` - Up 10 seconds
- `kafka` - Up 10 seconds  
- `schema-registry` - Up 10 seconds
- `connect` - Up 10 seconds
- `kafka-ui` - Up 10 seconds

**Important port note:**
- `9092` is the Kafka broker port, not a web page
- `9101` is the Kafka JMX port, not a web page
- browser UI is available at `http://localhost:8085`

## Step 2: Wait for Kafka to Fully Initialize (⚠️ CRITICAL!)

**This is the most important step.** Kafka takes 15-20 seconds to fully start.

```bash
# WAIT 20 SECONDS after docker compose up
# This gives Kafka time to bind to port 9092
sleep 20

# Then verify containers are ready
docker compose ps

# All 5 services should show "Up" status
```

**Why?** If you start the app before Kafka is ready, it will fail to connect with:
```
Connection to node 1 could not be established. Broker may not be available.
```

✅ **After 20 seconds, proceed to Step 3**

## Step 3: Verify Kafka Connectivity (2 minutes)

```bash
# Test Schema Registry
curl -s http://localhost:8081/subjects

# Should return: [] (empty array, no schemas yet)

# Open Kafka UI in browser
open http://localhost:8085
```

**Expected Output:**
- `[]` (empty JSON array)
- Kafka UI opens in your browser and shows the `local` cluster

## Step 4: Run Unit Tests (5 minutes)

```bash
mvn clean test
```

**Expected Output:**
```
BUILD SUCCESS
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
```

## Step 5: Start Application (10 minutes)

```bash
mvn spring-boot:run
```

**Expected Output:**
Watch for this line in logs:
```
Started LedgerLearningApplication in XX.XXX seconds
```

## Step 6: Test API (In another terminal)

Once app is running:

```bash
# Health check
curl http://localhost:8080/actuator/health

# Publish a transaction
curl -X POST http://localhost:8080/api/transactions \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId":"evt-quick-1",
    "tenantId":"quickstart",
    "accountId":"test-1",
    "amount":500.0,
    "direction":"CREDIT",
    "currency":"USD",
    "eventTime":1710000000000
  }'

# Wait for processing
sleep 2

# Query ledger
curl http://localhost:8080/api/ledger/quickstart/test-1
```

**Expected Output:**
Health check returns: `{"status":"UP"}`

Query returns:
```json
{
  "tenantId": "quickstart",
  "accountId": "test-1",
  "balance": 500.0,
  "currency": "USD",
  "lastEventTime": 1710000000000
}
```

## Step 7: Verify Schema Registry Schema Registration

```bash
curl -s http://localhost:8081/subjects
```

**Expected Output:**
Should now show registered schemas:
```json
["tx-events-value", "ledger-balances-value", "fraud-alerts-value"]
```

## Success Checklist

- [ ] All Docker containers are healthy
- [ ] mvn test passed
- [ ] App started without errors
- [ ] POST /api/transactions returns 202 ACCEPTED
- [ ] GET /api/ledger returns correct balance
- [ ] Schema Registry has registered Avro subjects

## Troubleshooting

If any step fails:

1. **Containers not starting:** Check `docker compose logs kafka` and `docker compose logs schema-registry`
2. **Tests failing:** Clear Maven cache: `rm -rf ~/.m2/repository && mvn clean test`
3. **App won't start:** Ensure port 8080 is available: `lsof -i :8080`
4. **Query returns 404:** Wait 5 more seconds, the state store might still be initializing

See `docs/testing-playbook.md` **Section 18: Troubleshooting Guide** for more detailed solutions.

## What's Next?

Once verification passes, proceed to Lab 1 in `docs/testing-playbook.md`:

```bash
# Review the comprehensive testing playbook
cat /Users/bojan/IdeaProjects/java-kafka/docs/testing-playbook.md
```

Start with:
- **Week 1:** Labs 1-3 (Environment, Ledger Correctness, Fraud Detection)
- **Week 2:** Labs 4-6 (Interactive Queries, EOS, Schema Evolution)
- Continue through Week 6 for full senior-level coverage

## Common Issues Quick Fixes

| Issue | Fix |
|-------|-----|
| Kafka not responding | `docker compose down && docker compose up -d && sleep 10` |
| Port 8080 in use | `lsof -i :8080 \| grep java \| awk '{print $2}' \| xargs kill` |
| Maven not found | Install: `brew install maven` (macOS) or follow Maven install docs |
| Stale Docker images | `docker compose pull && docker compose up -d` |
| State queries return 404 | Wait 5 seconds and try again; increase wait time in labs if needed |

---

**Time to full verification:** ~20 minutes

**Questions?** Refer to `docs/testing-playbook.md` Section 18+ for detailed troubleshooting.

