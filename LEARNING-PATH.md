# Learning Project Navigation & Curriculum

This document helps you navigate the learning materials and tracks your progress through the job-description-aligned curriculum.

## Quick Navigation

### First Time Here?
Start with: **`QUICKSTART.md`** (20 minutes)
- Verify your local environment
- Run first test and API calls
- Validate infrastructure

### Want Comprehensive Testing Labs?
Go to: **`docs/testing-playbook.md`** (6 weeks of structured practice)
- 14 labs covering all job-description skills
- Hands-on commands and expected outcomes
- Troubleshooting guide and quick reference

### Want Deep Technical Context?
Go to: **`docs/advanced-guide.md`** (senior-level deep dive)
- Design pattern explanations
- Extension points and learning exercises
- Multi-instance scaling, RocksDB tuning, cloud deployment
- Interview prep questions

### Quick Project Overview?
Go to: **`README.md`**
- Project summary and learning focus

### Want Topic/Schema Details?
Go to: **`docs/topics.md`**
- Topic design and state store strategy

---

## Learning Path by Role

### 👨‍💻 **Backend Engineer / Intermediate Level**
**Goal:** Master stateful streaming and Kafka basics  
**Time:** 2-3 weeks

```
Week 1: QUICKSTART.md + Labs 1-3 (Ledger, Fraud, Correctness)
Week 2: Labs 4-6 (Queries, EOS, Schema)
Week 3: Labs 7-9 (Connect, Multi-tenant, Resilience) + study the eventId dedup guard
```

**Artifacts to create:**
- Implementation notes on balance aggregation logic
- Fraud detection rule documentation
- Schema evolution test report

---

### 🏗️ **Staff/Senior Engineer / Lead Development**
**Goal:** Master architecture, leadership, and production-grade patterns  
**Time:** 6 weeks

```
Week 1: QUICKSTART.md + Labs 1-3
Week 2: Labs 4-6 + deep dive `docs/advanced-guide.md` Part 1-2
Week 3: Labs 7-9 + implement 2-3 extensions (Payment idempotency, Floor, Windowing)
Week 4: Labs 10-11 + RocksDB tuning lab from `advanced-guide.md` Part 4
Week 5: Labs 12-13 + write ADR for your topology
Week 6: Lab 14 + Kubernetes deployment + Terraform IaC
```

**Artifacts to create:**
- Architecture Decision Records (ADRs) for key choices
- Failure scenario test suite
- Kubernetes deployment manifests
- Terraform infrastructure code
- Operational runbook
- Monitoring/alerting strategy document

---

### 🎯 **Interview Preparation (Senior Role)**
**Goal:** Prepare for technical interviews  
**Time:** 4 weeks focused practice

```
Week 1: QUICKSTART.md + Labs 1-6 (Get comfortable with codebase)
Week 2: Labs 7-9 + `advanced-guide.md` Part 3, 5 (Multi-instance, Resilience)
Week 3: Labs 10-11 + Extensions from `advanced-guide.md` Part 2
Week 4: Deep dive: Part 4-8 of `advanced-guide.md` + Practice Part 8 interview questions
```

**Interview readiness checklist:**
- [ ] Can explain every line of `LedgerTopologyFactory.java`
- [ ] Can describe how exactly-once works and why it matters
- [ ] Can draw a diagram of state store changelog and recovery
- [ ] Can answer all 5 questions in Part 8 of `advanced-guide.md`
- [ ] Can explain multi-tenant keying strategy and alternatives
- [ ] Can discuss RocksDB tuning trade-offs
- [ ] Can describe production deployment considerations

---

## Skill-to-Lab Cross-Reference

Use this table to find labs for specific skills from the job description.

| Skill | Labs | Files | Time |
|-------|------|-------|------|
| **Java + Spring Boot + Kafka Streams** | 1, 2, 3, 4 | `LedgerTopologyFactory.java`, `KafkaStreamsConfig.java` | 2 hrs |
| **Stateful streaming & state stores** | 3, 4, 8, 11 | `FraudDetectionProcessor.java`, `TransactionGuardProcessor.java`, `LedgerQueryService.java` | 3 hrs |
| **Interactive queries (IQ)** | 4, 8 | `LedgerQueryService.java`, `InteractiveQueryRouter.java`, REST controllers | 1 hr |
| **Exactly-once semantics (EOS)** | 5, 9 | `application.yml` config, failure scenarios | 2 hrs |
| **Schema Registry + Avro** | 6, 12 | Avro schema files, `KafkaStreamsConfig.java` | 1.5 hrs |
| **Kafka Connect** | 7 | Connector configs in `connect/` | 1 hr |
| **Financial logic** | 2, 3, 10 | `LedgerTopologyFactory.java`, `model/Money.java`, `FraudDetectionProcessor.java` | 2 hrs |
| **Multi-tenant SaaS patterns** | 8, 13 | Keying strategy, `security/TenantAccess.java`, isolation validation | 1.5 hrs |
| **Resilience & failure handling** | 9, 11 | Failure labs, rebalance behavior | 2 hrs |
| **RocksDB tuning** | 11, 14 | `advanced-guide.md` Part 4, performance labs | 3 hrs |
| **Cloud/K8s/Terraform** | 14 | `k8s/`, `terraform/`, `advanced-guide.md` Part 6 | 4 hrs |
| **Leadership/mentoring** | 13 | ADR writing, design review prep | 2 hrs |

---

## File Structure & Purpose

```
java-kafka/
├── README.md                          # Project overview + getting started
├── QUICKSTART.md                      # ← START HERE (20 min verification)
├── pom.xml                            # Maven build config
├── docker-compose.yml                 # Local Kafka infrastructure
│
├── src/main/java/com/example/ledger/
│   ├── LedgerLearningApplication.java # Spring Boot entry point
│   ├── api/
│   │   ├── TransactionIngestController.java    # Publish endpoint
│   │   ├── LedgerQueryController.java          # Read balance
│   │   └── FraudQueryController.java           # Read fraud state
│   ├── config/
│   │   ├── KafkaStreamsConfig.java             # Serdes, topology wiring
│   │   ├── LedgerStreamProperties.java         # App config
│   │   └── TopicConfig.java                    # Topic auto-creation
│   ├── model/
│   │   ├── TransactionRequest.java             # API input
│   │   ├── BalanceView.java                    # Balance state view
│   │   ├── FraudStateView.java                 # Fraud state view
│   │   └── AccountActivity.java                # Fraud detector state
│   ├── serde/
│   │   └── JsonSerde.java                      # Custom JSON serializer
│   └── streams/
│       ├── LedgerTopologyFactory.java          # ← CORE TOPOLOGY
│       ├── TransactionGuardProcessor.java      # ← DEDUP + CURRENCY GUARD
│       ├── FraudDetectionProcessor.java        # ← PROCESSOR API EXAMPLE
│       ├── LedgerQueryService.java             # ← INTERACTIVE QUERIES
│       ├── InteractiveQueryRouter.java         # ← CROSS-INSTANCE ROUTING
│       └── Topics.java                         # Topic name constants
│
├── src/main/avro/
│   ├── TransactionEvent.avsc                   # Input event schema
│   ├── LedgerBalance.avsc                      # Balance state schema
│   └── FraudAlert.avsc                         # Fraud alert schema
│
├── src/main/resources/
│   └── application.yml                         # Spring/Kafka config (EOS, broker, etc.)
│
├── src/test/java/com/example/ledger/streams/
│   └── LedgerTopologyTest.java                 # Topology unit tests
│
├── connect/
│   ├── ledger-source.json                      # Source connector config
│   └── ledger-sink.json                        # Sink connector config
│
└── docs/
    ├── topics.md                               # Topic strategy & state stores
    ├── testing-playbook.md                     # ← COMPREHENSIVE 14 LABS (6 weeks)
    └── advanced-guide.md                       # ← SENIOR-LEVEL DEEP DIVE (8 parts)
```

---

## Weekly Progression Template

Pick your path above and follow this template:

### Week Template

```markdown
## Week X: [Theme]

### Labs to Complete
- [ ] Lab N: [Name]
- [ ] Lab M: [Name]

### Extension/Stretch Goals
- [ ] Challenge from advanced-guide.md

### Artifacts to Create
- [ ] Document or test result
- [ ] Code change or measurement

### Reflection Questions
1. What was the hardest part?
2. What surprised you?
3. How would you explain this to a junior engineer?

### Time Estimate
- Lectures/reading: X hours
- Hands-on labs: X hours
- Extensions: X hours
```

---

## Self-Assessment Rubric

Rate yourself on each skill (1-5):

| Skill | 1 | 2 | 3 | 4 | 5 |
|-------|---|---|---|---|---|
| **Kafka Streams DSL** | Confused | Following docs | Write simple streams | Build production topologies | Architect complex flows |
| **Processor API** | Never heard of it | Seen examples | Implemented simple transformer | Complex stateful logic | Design patterns mastery |
| **State Stores** | Don't understand | Know basics | Use effectively | Tune for performance | Multi-instance strategy |
| **Interactive Queries** | Not attempted | Basic single-node | Implemented | Understand IQ limitations | Distributed routing design |
| **Exactly-Once** | Vague concept | Know it exists | Config enabled | Understand guarantees | Failure scenario expert |
| **Multi-Tenant Architecture** | Single-tenant only | Thought about isolation | Keying strategy implemented | Full isolation, hot-tenant handling | Quota/fairness system |
| **RocksDB Tuning** | Default config | Know cache/commit trade-off | Basic measurement | Custom RocksDBConfigSetter | Optimize for latency profile |
| **Failure Scenarios** | Never tested | Manual failures | Systematic failure labs | Measurement & metrics | Incident response playbook |
| **Cloud Deployment** | Local only | Docker aware | K8s basics | Helm/Terraform | Full IaC + GitOps ready |

**Target:** All skills at 4-5 before a senior interview.

---

## Documentation by Use Case

### 🚀 "I just want to get it running"
→ `QUICKSTART.md`

### 🧪 "I want to learn by doing"
→ `docs/testing-playbook.md` (Labs 1+)

### 🏛️ "I want to understand the design"
→ `docs/advanced-guide.md` (Parts 1-3)

### ⚡ "I want to optimize performance"
→ `docs/advanced-guide.md` (Part 4) + Lab 11

### 🌍 "I want to deploy to production"
→ `docs/advanced-guide.md` (Parts 6-7)

### 🎤 "I have an interview coming"
→ `docs/advanced-guide.md` (Part 8) + interview questions

### 🔧 "Something is broken"
→ `docs/testing-playbook.md` (Section 18: Troubleshooting)

---

## Evidence Checklist for Portfolio

Track these artifacts to prove senior readiness:

### After Week 1
- [ ] Successful QUICKSTART.md run (screenshot)
- [ ] Completed Labs 1-3 (notes)
- [ ] First extension implemented (payment idempotency or floor logic)

### After Week 2
- [ ] Labs 4-6 completed (notes)
- [ ] Schema evolution test report
- [ ] API contract documentation

### After Week 3
- [ ] Labs 7-9 completed (notes)
- [ ] Multi-tenant isolation test proof
- [ ] Failure scenario lab results

### After Week 4
- [ ] Lab 10-11 completed
- [ ] RocksDB tuning measurements
- [ ] Performance benchmark report

### After Week 5
- [ ] Lab 12-13 completed
- [ ] Architecture Decision Record (ADR) written
- [ ] Code review rubric defined

### After Week 6
- [ ] Lab 14 completed
- [ ] Kubernetes deployment YAML
- [ ] Terraform infrastructure code
- [ ] Operational runbook draft

---

## Recommended Study Order

### For maximum learning retention:

1. **Read** the overview in this file (5 min)
2. **Skim** `README.md` for context (5 min)
3. **Do** `QUICKSTART.md` and verify everything works (20 min)
4. **Read** Part 1 of `advanced-guide.md` (15 min)
5. **Do** Labs 1-3 from `testing-playbook.md` (2 hours)
6. **Reflect** on what you learned and how it connects
7. **Repeat** for next set of labs

This pattern (Read → Do → Reflect) maximizes retention.

---

## Tracking Your Progress

Copy this checklist and update it as you progress:

```markdown
# My Learning Progress

## Current Week: __
## Target Completion: __ / 6 weeks

### Week 1
- [ ] QUICKSTART.md verified ✅
- [ ] Lab 1: Environment Smoke Test ✅
- [ ] Lab 2: Ledger Correctness ✅
- [ ] Lab 3: Fraud Detection ✅
- [ ] Notes written in: `my-notes/week1.md`

### Week 2
- [ ] Lab 4: Interactive Queries
- [ ] Lab 5: EOS Verification
- [ ] Lab 6: Schema Registry
- [ ] Extension: Schema evolution test
- [ ] Notes written in: `my-notes/week2.md`

... (continue for weeks 3-6)

## Skills at Rubric Level
- Kafka Streams DSL: 3/5
- Processor API: 2/5
- State Stores: 3/5
... (etc)

## Interview Prep Checklist
- [ ] Can explain LedgerTopologyFactory.java line-by-line
- [ ] Can answer 5 Part-8 interview questions
... (etc)
```

---

## Getting Help

### First Step
Check `docs/testing-playbook.md` **Section 18: Troubleshooting Guide**

### For Concept Questions
- Kafka Streams docs: https://kafka.apache.org/documentation/streams/
- Confluent blog: https://www.confluent.io/blog/
- This project's `advanced-guide.md`

### For Implementation Help
- Review source code: `src/main/java/com/example/ledger/streams/`
- Check test examples: `src/test/java/`
- Look at ADR templates in this doc

### For Environment Issues
- Docker: https://docs.docker.com/
- Confluent Docker images: https://docs.confluent.io/platform/current/installation/docker/overview.html

---

## Summary

This learning project is designed to take you from intermediate to **senior Kafka engineer level** with emphasis on:

✅ Stateful streaming (state stores, IQ)  
✅ Processor API mastery  
✅ Exactly-once semantics  
✅ Multi-tenant SaaS patterns  
✅ Production resilience & operations  
✅ Cloud deployment readiness  
✅ Leadership & mentoring  

**Total time investment:** 30-40 hours over 6 weeks  
**Expected outcome:** Ready for senior engineer interviews and production deployments

**Start now:** `QUICKSTART.md` → 20 minutes → You'll see it working

Good luck! 🚀

