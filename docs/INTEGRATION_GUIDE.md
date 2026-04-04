# Integration Guide — Canton Compliance SDK

## 1. Install the SDK
```bash
npm install @canton/compliance-sdk
```

## 2. Initialise the client
```typescript
import { ComplianceClient } from '@canton/compliance-sdk';

const compliance = new ComplianceClient(
  process.env.CANTON_JSON_API_URL!,
  process.env.CANTON_JWT!,
);
```

## 3. Check a counterparty before transacting
```typescript
const allowed = await compliance.isAllowed(counterpartyId);
if (!allowed) throw new Error('Compliance check failed');
// ... proceed with transaction
```

## 4. Configure rules
Deploy the `ComplianceGate` Daml contract with your rule set:
```bash
daml script --dar .daml/dist/canton-compliance-sdk-0.1.0.dar \
  --script-name ComplianceTest:deployGate \
  --ledger-host localhost --ledger-port 6865
```
