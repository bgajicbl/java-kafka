# Documentation Index

Complete reference to all learning materials in this project.

## 📚 Core Documents (Read First)

| Document | Duration | Purpose | For Whom |
|----------|----------|---------|----------|
| **`README.md`** | 5 min | Project overview + quick links | Everyone |
| **`QUICKSTART.md`** | 20 min | Verify environment + first test | First-time users |
| **`LEARNING-PATH.md`** | 15 min | Choose your learning path + navigation | All learners |
| **`docs/application-architecture-and-kafka-theory.md`** | 45-90 min | Full architecture, internals, integrations, Kafka/KStreams theory | Backend+ |

## 🧪 Comprehensive Learning

| Document | Duration | Labs | Purpose | Path |
|----------|----------|------|---------|------|
| **`docs/testing-playbook.md`** | 6 weeks | 14 labs | Hands-on practice for all role skills | Everyone |
| Includes: | | | | |
| - Lab 1: Environment Smoke Test | 15 min | 1 | Set up infrastructure | All |
| - Lab 2: Ledger Correctness | 30 min | 1 | Test balance logic | Backend+ |
| - Lab 3: Fraud Detection | 45 min | 1 | Processor API + state | Backend+ |
| - Lab 4: Interactive Queries | 30 min | 1 | RocksDB-backed reads | Backend+ |
| - Lab 5: EOS Verification | 45 min | 1 | Exactly-once semantics | Backend+ |
| - Lab 6: Schema Registry | 45 min | 1 | Avro evolution | Backend+ |
| - Lab 7: Kafka Connect | 45 min | 1 | Connector lifecycle | Backend+ |
| - Lab 8: Multi-Tenant SaaS | 30 min | 1 | Tenant isolation | Staff+ |
| - Lab 9: Failure & Recovery | 45 min | 1 | Resilience testing | Staff+ |
| - Lab 10: Financial Integrity | 1-2 hrs | 1 | Domain logic scenarios | Staff+ |
| - Lab 11: RocksDB Tuning | 1-2 hrs | 1 | Performance optimization | Staff+ |
| - Lab 12: CI Gate & Governance | 1 hr | 1 | Schema contracts | Staff+ |
| - Lab 13: Leadership Exercises | 2-3 hrs | 1 | ADRs, design review | Staff+ |
| - Lab 14: Cloud/K8s/Terraform | 3-4 hrs | 1 | Production deployment | Staff+ |
| **Sections 15-20:** | | | | |
| - Section 15: Weekly progression | Ref | - | 6-week study plan | All |
| - Section 16: Portfolio checklist | Ref | - | Track evidence | All |
| - Section 17: Project boundaries | Ref | - | What to expect | All |
| - Section 18: Troubleshooting | Ref | - | Fix common issues | All |
| - Section 19: Quick reference | Ref | - | Copy-paste commands | All |
| - Section 20: When to seek help | Ref | - | Escalation guide | All |

## 🏛️ Advanced Topics

| Document | Duration | Topics | Purpose | Path |
|----------|----------|--------|---------|------|
| **`docs/advanced-guide.md`** | 10+ hrs | 8 parts | Senior-level deep dive | Staff+ |
| Part 1: Design Patterns | 1 hr | State stores, Processor API, Multi-tenant, EOS | Understand "why" | All |
| Part 2: Extensions | 2-3 hrs | Event dedup, balance floor, windowing, schema evolution, quotas | Hands-on learning | Backend+ |
| Part 3: Multi-Instance IQ | 1 hr | Metadata-aware routing, Redis cache | Distributed queries | Staff+ |
| Part 4: RocksDB Tuning | 1-2 hrs | Cache/commit trade-offs, custom config | Performance | Staff+ |
| Part 5: Failure Scenarios | 1 hr | Rebalancing, EOS during failure | Resilience | Staff+ |
| Part 6: Cloud Deployment | 2 hrs | K8s manifest, Terraform IaC | Production | Staff+ |
| Part 7: Observability | 1 hr | Metrics, alerting, dashboards | Operations | Staff+ |
| Part 8: Interview Prep | 30 min | 5 key questions + answers | Hiring prep | Interview+ |

## 📋 Reference Docs

| Document | Duration | Contents | Use Case |
|----------|----------|----------|----------|
| **`docs/topics.md`** | 5 min | Topic design, state stores, naming | Understand data model |
| **`pom.xml`** | 5 min | Maven build config, dependencies | Build configuration |
| **`docker-compose.yml`** | 5 min | Infrastructure services | Local environment |

## 🔍 Code Structure

All implementation examples are in source code:

```
src/main/java/com/example/ledger/
├── streams/
│   ├── LedgerTopologyFactory.java    (Core topology: DSL + Processor API)
│   ├── FraudDetectorTransformer.java (Processor API example)
│   ├── LedgerQueryService.java       (Interactive queries)
│   └── Topics.java                   (Topic constants)
├── config/
│   ├── KafkaStreamsConfig.java       (Streams wiring)
│   ├── TopicConfig.java              (Topic creation)
│   └── LedgerStreamProperties.java   (Configuration)
└── api/
    ├── TransactionIngestController.java
    ├── LedgerQueryController.java
    └── FraudQueryController.java
```

## 📊 Learning Paths (Recommended Sequences)

### Path A: Backend Engineer (2-3 weeks)
```
Day 1: QUICKSTART.md (20 min)
Day 1-2: Labs 1-3 (2.5 hours) 
Day 3-4: Labs 4-6 (2 hours)
Day 5-6: Labs 7-9 (2.5 hours)
Day 7: Extension: Event deduplication
```

**Time investment:** 10-12 hours  
**Outcome:** Proficient with Kafka Streams + state stores

### Path B: Staff/Senior Engineer (6 weeks)
```
Week 1: QUICKSTART.md + Labs 1-3
Week 2: Labs 4-6 + advanced-guide.md Parts 1-2
Week 3: Labs 7-9 + Extension: 2-3 features
Week 4: Labs 10-11 + RocksDB tuning lab
Week 5: Labs 12-13 + Write ADR
Week 6: Lab 14 + K8s + Terraform
```

**Time investment:** 35-40 hours  
**Outcome:** Senior-ready for production and team leadership

### Path C: Interview Preparation (4 weeks)
```
Week 1: QUICKSTART.md + Labs 1-6
Week 2: Labs 7-9 + advanced-guide.md Parts 3, 5
Week 3: Labs 10-11 + Extensions
Week 4: advanced-guide.md Parts 4, 6-8 + Practice interview Q&A
```

**Time investment:** 20-25 hours  
**Outcome:** Confident for senior engineering interviews

## 🎯 By Topic

### Core Kafka Streams
- `README.md` - Overview
- `QUICKSTART.md` - First run
- `docs/application-architecture-and-kafka-theory.md` - End-to-end architecture and theory mapping
- `docs/testing-playbook.md` - Labs 1-4
- `docs/advanced-guide.md` - Part 1

### Exactly-Once Semantics
- `docs/testing-playbook.md` - Lab 5, Troubleshooting section
- `docs/advanced-guide.md` - Part 1.4, Part 5
- Source: `application.yml` (processing.guarantee config)

### State Stores & Interactive Queries
- `docs/application-architecture-and-kafka-theory.md` - Sections 6-8 and 10.4
- `docs/testing-playbook.md` - Labs 3-4, 8
- `docs/advanced-guide.md` - Part 1.1, 3-4
- Source: `LedgerQueryService.java`

### Processor API & Custom Logic
- `docs/testing-playbook.md` - Lab 3
- `docs/advanced-guide.md` - Part 1.2, 2.1-2.5
- Source: `FraudDetectorTransformer.java`

### Multi-Tenant SaaS Patterns
- `docs/testing-playbook.md` - Lab 8
- `docs/advanced-guide.md` - Part 1.3, 2.5
- Source: Key format in `LedgerTopologyFactory.java`

### Schema Registry & Evolution
- `docs/application-architecture-and-kafka-theory.md` - Sections 5 and 12.1
- `docs/testing-playbook.md` - Lab 6, Troubleshooting
- `docs/advanced-guide.md` - Part 2.4
- Source: `src/main/avro/*.avsc` files

### Kafka Connect
- `docs/testing-playbook.md` - Lab 7
- Source: `connect/` config files

### RocksDB Performance
- `docs/testing-playbook.md` - Lab 11
- `docs/advanced-guide.md` - Part 4
- Source: `application.yml` (cache.max.bytes.buffering, commit.interval.ms)

### Failure & Resilience
- `docs/testing-playbook.md` - Lab 9, Section 18
- `docs/advanced-guide.md` - Part 5
- Hands-on: Kill containers and observe recovery

### Cloud Deployment
- `docs/advanced-guide.md` - Part 6-7
- Hands-on: Create K8s manifests and Terraform modules

### Leadership & Architecture
- `docs/testing-playbook.md` - Lab 13
- `docs/advanced-guide.md` - Part 1-8 (all design rationale)
- Hands-on: Write ADRs and design reviews

---

## 📈 Progress Tracking

Use this checklist to track completion:

### Phase 1: Foundation
- [ ] README.md read
- [ ] QUICKSTART.md completed
- [ ] LEARNING-PATH.md reviewed
- [ ] Labs 1-3 done
- [ ] First API call working

### Phase 2: Core Skills
- [ ] Labs 4-6 done
- [ ] Schema Registry tested
- [ ] Avro schemas understood
- [ ] EOS behavior understood

### Phase 3: Advanced Features
- [ ] Labs 7-9 done
- [ ] Multi-tenant isolation verified
- [ ] Failure scenarios tested
- [ ] RocksDB metrics understood

### Phase 4: Production Readiness
- [ ] Labs 10-14 done
- [ ] Extensions implemented (2-3)
- [ ] ADR written for topology
- [ ] K8s manifest created
- [ ] Terraform IaC drafted

### Phase 5: Interview Ready
- [ ] Part 8 questions answered
- [ ] Can explain line-by-line: `LedgerTopologyFactory.java`
- [ ] Can draw: State store architecture
- [ ] Can discuss: Multi-instance IQ routing
- [ ] Can solve: RocksDB tuning scenarios

---

## 🆘 Troubleshooting Reference

**Problem** | **Solution**
-----------|------------
Can't connect to Kafka | `docs/testing-playbook.md` Section 18, Issue 1
mvn test failing | Section 18, Issue 5
Schema Registry empty | Section 18, Issue 8
State queries return 404 | Section 18, Issue 7
App won't start | Section 18, Issue 3
Fraud alerts not triggering | Section 18, Issue 9

---

## 🎓 Self-Assessment Rubric

After completing your chosen path, rate yourself:

| Skill | Beginner | Intermediate | Proficient | Expert |
|-------|----------|--------------|------------|--------|
| Kafka Streams DSL | Know what it is | Can follow examples | Write production topologies | Mentor others |
| Processor API | Never used | Implemented once | Complex stateful logic | Framework patterns |
| State Stores | Aware | Basic use | Tune performance | Multi-instance design |
| Interactive Queries | Don't know | Single node | Implement correctly | Distributed routing |
| EOS | Buzzword | Configuration | Understand guarantees | Failure scenarios |
| Multi-Tenant | Single tenant | Thought about it | Full isolation | Quota system |
| RocksDB | Default config | Know trade-offs | Basic tuning | Custom ConfigSetter |
| Cloud/K8s | Local only | Understand basics | Helm/Terraform | GitOps ready |

**Target:** All at "Proficient" or higher

---

## 📞 Support Resources

### Internal Documentation
- See: `docs/testing-playbook.md` Section 20 (When to Seek Help)

### External Resources
- Kafka Streams: https://kafka.apache.org/documentation/streams/
- Confluent Docs: https://docs.confluent.io/
- Schema Registry: https://docs.confluent.io/platform/current/schema-registry/
- RocksDB: https://github.com/facebook/rocksdb/wiki

### Getting Unstuck
1. Check `docs/testing-playbook.md` Section 18 (Troubleshooting)
2. Review `docs/advanced-guide.md` for deeper context
3. Inspect source code in `src/main/java/com/example/ledger/`
4. Check test examples in `src/test/java/`

---

## 📝 Version & Maintenance

**Last Updated:** March 2026  
**Maintainer:** Learning Content  
**Status:** Production-ready for learning  

**Compatibility:**
- Java 17+
- Spring Boot 3.2.5
- Kafka 7.6.1
- Confluent Platform 7.6.1

---

## Quick Links

👉 **Start here:** `QUICKSTART.md`  
📚 **Full curriculum:** `LEARNING-PATH.md`  
🧪 **Labs:** `docs/testing-playbook.md`  
🏗️ **Architecture:** `docs/advanced-guide.md`  
❓ **Help:** `docs/testing-playbook.md` Section 18  

Good luck with your learning journey! 🚀

