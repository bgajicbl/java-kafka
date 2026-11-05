# Project Architecture & Learning Map

## 🏗️ Application Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Spring Boot Application                       │
│                  (Port 8080, EOS enabled)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────────┐         ┌──────────────────────┐          │
│  │  REST API Layer  │         │  Interactive Query   │          │
│  ├──────────────────┤         │      Layer           │          │
│  │ POST /api/tx     │         ├──────────────────────┤          │
│  │ GET /api/ledger  │         │ GET /api/ledger/{t}  │          │
│  │ GET /api/fraud   │         │ GET /api/fraud/{t}   │          │
│  └────────┬─────────┘         └──────────┬───────────┘          │
│           │                              │                       │
│           └──────────┬───────────────────┘                       │
│                      │                                           │
│              ┌───────▼────────┐                                  │
│              │ Kafka Streams  │                                  │
│              │   Topology     │                                  │
│              └───────┬────────┘                                  │
│                      │                                           │
│        ┌─────────────┼─────────────┐                             │
│        │             │             │                             │
│    ┌───▼──┐   ┌──────▼────┐   ┌───▼──────┐                      │
│    │ DSL  │   │ Processor  │   │ State    │                      │
│    │ Agg. │   │ API        │   │ Stores   │                      │
│    │      │   │ (Fixed-Key)│   │ (RocksDB)│                      │
│    └───┬──┘   └──────┬─────┘   └────┬────┘                      │
│        │             │              │                           │
│        └─────────────┼──────────────┘                            │
│                      │                                           │
└──────────────────────┼───────────────────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          │            │            │
    ┌─────▼──┐   ┌────▼──┐   ┌────▼──┐
    │tx-     │   │ledger-│   │fraud- │
    │events  │   │balance│   │alerts │
    │(input) │   │(change)   │       │
    └────────┘   └────────┘   └───────┘
          │            │            │
          └────────────┼────────────┘
                       │
              Kafka Cluster (Broker, SR)
```

---

## 📚 Learning Journey Map

```
START HERE
    │
    ▼
┌─────────────────┐
│   README.md     │  ← Project overview (5 min)
│                 │
└────────┬────────┘
         │
         ▼
┌──────────────────────┐
│  QUICKSTART.md       │  ← Environment verification (20 min)
│ (6-step quick test)  │
└────────┬─────────────┘
         │
         ▼
    CHOOSE YOUR PATH
         │
    ┌────┼────┬─────────────────┐
    │    │    │                 │
    ▼    ▼    ▼                 ▼
   A    B    C                 D
   │    │    │                 │
```

### Path A: Quick Learning (Weeks 1-3)
```
Labs 1-9 (20-30 hours)
├─ Week 1: Labs 1-3 (foundations)
│  ├─ Environment
│  ├─ Balance logic
│  └─ Fraud detection
├─ Week 2: Labs 4-6 (core skills)
│  ├─ Interactive queries
│  ├─ EOS
│  └─ Schema Registry
└─ Week 3: Labs 7-9 (integration)
   ├─ Kafka Connect
   ├─ Multi-tenant
   └─ Resilience

Outcome: Proficient Kafka engineer
```

### Path B: Senior Engineer (Weeks 1-6)
```
Labs 1-14 + Advanced Guide (35-40 hours)
├─ Weeks 1-3: Labs 1-9 + Part 1 of advanced-guide
│  └─ Core skills + design patterns
├─ Week 4: Labs 10-11 + Part 4 of advanced-guide
│  └─ Financial logic + RocksDB tuning
├─ Week 5: Labs 12-13 + Part 8 of advanced-guide
│  └─ Governance + leadership
└─ Week 6: Lab 14 + Parts 6-7 of advanced-guide
   └─ Cloud deployment + ops

Outcome: Senior-ready engineer
```

### Path C: Interview Prep (Weeks 1-4)
```
Labs 1-11 + Advanced Guide Part 8 (20-25 hours)
├─ Week 1: Labs 1-6
│  └─ Foundation + core skills
├─ Week 2: Labs 7-9 + Advanced Part 3, 5
│  └─ Integration + resilience
├─ Week 3: Labs 10-11 + Advanced Part 2, 4
│  └─ Domain logic + performance
└─ Week 4: Advanced Parts 4, 6-8
   └─ Tuning + interview prep

Outcome: Interview ready
```

### Path D: Deep Dive (10+ weeks)
```
All Labs + All Advanced Topics + Extensions
├─ Weeks 1-6: Complete path B (senior engineer)
├─ Weeks 7-8: Implement 3-5 extensions
│  ├─ Event deduplication
│  ├─ Balance floor enforcement
│  ├─ Windowed aggregations
│  └─ Quota enforcement
├─ Weeks 9-10: Deploy to Kubernetes + Terraform
│  ├─ Write K8s manifests
│  ├─ Create Terraform modules
│  └─ Set up observability
└─ Week 10+: Write operational docs
   ├─ ADRs (architecture decisions)
   ├─ Runbooks
   └─ Team mentoring guides

Outcome: Expert-level mastery
```

---

## 🎯 Skill Development Path

```
Day 1
│
├─ QUICKSTART.md (verify environment) ................... 20 min
│
Day 2-3
├─ Lab 1: Environment ................................... 15 min
├─ Lab 2: Balance Logic ................................. 30 min
├─ Lab 3: Fraud Detection ............................... 45 min
├─ Total Week 1 .......................................... 2.5 hrs
│
Day 4-5
├─ Lab 4: Interactive Queries ........................... 30 min
├─ Lab 5: EOS ............................................ 45 min
├─ Lab 6: Schema Registry ............................... 45 min
├─ Total Week 2 .......................................... 2 hrs
│
Day 6-7
├─ Lab 7: Kafka Connect ................................. 45 min
├─ Lab 8: Multi-Tenant .................................. 30 min
├─ Lab 9: Failure & Recovery ............................ 45 min
├─ Total Week 3 .......................................... 2.5 hrs
│
By Week 3: You're proficient with core Kafka Streams skills
│
Week 4+: Advanced topics (if pursuing senior role or interview prep)
├─ RocksDB tuning
├─ Multi-instance IQ routing
├─ Cloud deployment
└─ Leadership patterns
```

---

## 📖 Documentation Dependencies

```
Start Here
    │
    ├─ README.md
    │   │
    │   ├─ QUICKSTART.md ─────────────┐
    │   │                              │
    │   ├─ LEARNING-PATH.md ◄──────────┤─ Choose Path
    │   │                              │
    │   └─ PROJECT-SUMMARY.md ────────┤
    │                                 │
    ├─────────────────────────────────┤
    │                                 │
    └─► Choose Your Learning Path ───┤
            │                         │
        ┌───┼───┬──────────────┐     │
        │   │   │              │     │
        ▼   ▼   ▼              ▼     │
        │   │   │              │     │
        │   │   │   Advanced   │     │
        │   │   │   Guide      │     │
        │   │   │   Part 1     │     │
        │   │   │              │     │
        │   │   └─────────────►│     │
        │   │                  │     │
    └───┴───┴──────────────────┴────►
            Testing Playbook
            (14 Labs)
                │
                ├─ Lab-specific
                │  guides
                │
                ├─ Part 2-8 of
                │  Advanced Guide
                │
                ├─ Troubleshooting
                │  (Section 18)
                │
                └─ Quick Reference
                   (Section 19)
```

---

## 🔄 Topic Coverage Progression

```
Week 1-2: Fundamentals
├─ Kafka Streams basics (DSL)
├─ Balance aggregation
├─ Fraud detection patterns
├─ Interactive queries (single instance)
└─ Exactly-once semantics

Week 3-4: Core Production Skills
├─ Processor API transformers
├─ Multi-tenant isolation
├─ Failure scenarios & recovery
├─ Schema Registry governance
└─ RocksDB state store basics

Week 5-6: Advanced Topics (if pursuing senior role)
├─ Multi-instance query routing
├─ RocksDB performance tuning
├─ Cloud deployment (K8s + Terraform)
├─ Operational observability
└─ Leadership & architecture patterns
```

---

## 📊 Effort vs. Skill Gain

```
40 hours
│      /
│     /  ← Senior engineer (full path)
│    /
│   /
30│  /
│ /
│/     ← Interview ready (4 week path)
20│────────/
│       /
│      /
│     /
10│   /
│  /   ← Proficient (3 week path)
│ /
│/────────► Quick start (20 min)
0└─┬──┬──┬──┬──┬──┬──┬──
  1  2  3  4  5  6  7  weeks
```

---

## 🎓 Self-Assessment Checkpoints

```
After QUICKSTART (Day 1)
├─ ✅ Docker containers running
├─ ✅ First API call works
└─ ✅ Maven tests pass

After Lab 3 (Week 1)
├─ ✅ Can explain balance aggregation
├─ ✅ Understand Processor API
└─ ✅ Know multi-tenant keying

After Lab 6 (Week 2)
├─ ✅ Can explain EOS
├─ ✅ Understand schema evolution
└─ ✅ Can deploy Kafka Connect

After Lab 9 (Week 3)
├─ ✅ Confident with core skills
├─ ✅ Can troubleshoot failures
└─ ✅ Ready for backend role

After Lab 14 (Week 6)
├─ ✅ Architect production topologies
├─ ✅ Deploy to cloud platforms
├─ ✅ Lead design reviews
└─ ✅ Ready for senior interviews
```

---

## 🚀 Quick Navigation

```
I want to...                               Go to...
─────────────────────────────────────────────────────
Get it running NOW (20 min)           → QUICKSTART.md
Understand the project                 → README.md
Plan my 6-week journey                 → LEARNING-PATH.md
See what's in the box                  → PROJECT-SUMMARY.md
Find documentation                     → docs/INDEX.md or DOCUMENTATION.md
Find a specific topic                   → docs/INDEX.md (cross-ref table)
Get unstuck                             → docs/testing-playbook.md Sec. 18
Practice hands-on labs                  → docs/testing-playbook.md Labs 1-14
Understand the design                   → docs/advanced-guide.md Part 1
Prepare for interviews                  → docs/advanced-guide.md Part 8
Learn advanced topics                   → docs/advanced-guide.md Parts 2-7
Read data model details                 → docs/topics.md
Troubleshoot Docker/infrastructure      → docs/testing-playbook.md Sec. 18.1-3
```

---

## ✅ Success Checklist

```
After Completion:

Technical Skills
├─ ✅ Can build Kafka Streams topologies
├─ ✅ Understand state stores & IQ
├─ ✅ Know exactly-once semantics deeply
├─ ✅ Implement Processor API transformers
├─ ✅ Design multi-tenant applications
├─ ✅ Tune RocksDB for performance
└─ ✅ Deploy to production platforms

Portfolio Artifacts
├─ ✅ Working application with tests
├─ ✅ Extension implementations (2-3)
├─ ✅ Architecture decision records
├─ ✅ Kubernetes manifests
├─ ✅ Terraform modules
└─ ✅ Operational documentation

Interview Ready
├─ ✅ Can explain every design choice
├─ ✅ Comfortable with all 14 labs
├─ ✅ Answer 5 Part-8 questions
├─ ✅ Ready for senior role questions
└─ ✅ Have portfolio to show
```

---

## 🎉 Final Outcome

```
                    SENIOR KAFKA ENGINEER
                    
Your Path:          30-40 hours of focused learning
Your Timeline:      6 weeks part-time (2-3 hrs/week)
Your Outcome:       
                    ✅ Production-ready expert
                    ✅ Interview confident
                    ✅ Team ready (mentoring)
                    ✅ Architecture capable
                    ✅ Cloud deployment ready
                    
Your Evidence:      Portfolio of artifacts
                    - Code implementations
                    - Architecture docs
                    - Test suites
                    - Infrastructure code
                    - Design rationale (ADRs)

Your Network:       Understanding of:
                    - Kafka ecosystem
                    - SaaS architecture
                    - Financial systems
                    - Cloud platforms
                    - Team leadership
```

---

You have everything you need. Start with `QUICKSTART.md` and progress at your own pace. 🚀

