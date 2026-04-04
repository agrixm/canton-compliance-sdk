# Canton Compliance SDK

An open-source compliance SDK for Canton that automates KYC/AML rule enforcement
directly within smart contracts.

## Features
- Pluggable compliance rule engine in Daml
- KYC credential verification workflow
- AML transaction screening hooks
- FATF Travel Rule implementation
- Compliance dashboard UI
- REST API for rule configuration

## Quick Start
```bash
daml build
daml test
npm run dev   # start compliance dashboard
```

## Architecture
Rules are encoded as Daml contracts. The SDK exposes a TypeScript client
to check counterparty compliance before executing any transaction.
